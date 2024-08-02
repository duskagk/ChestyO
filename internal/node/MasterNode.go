package node

// internal/node/MasterNode.go

import (
	policy "ChestyO/internal/enum"
	"ChestyO/internal/transport"
	"ChestyO/internal/utils"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type DataNodeInfo struct {
    ID      string
    Addr    string
    // Conn    net.Conn
}

type MasterNode struct {
    ID            string
    dataNodes     map[string]*DataNodeInfo
    fileLocations map[string][]string
    userMutexes   sync.Map
    transport     *transport.TCPTransport
    mu            sync.RWMutex
    stopChan      chan struct{}
}

func NewMasterNode(id string) *MasterNode {
	return &MasterNode{
		ID:            id,
		dataNodes:     make(map[string]*DataNodeInfo),
		fileLocations: make(map[string][]string),
		userMutexes: sync.Map{},
        stopChan: make(chan struct{}),
	}
}

func (m *MasterNode)TCPProtocl(ctx context.Context, conn net.Conn){
    log.Printf("Master Node : get Tcp Protocol")
    m.handleConnection(ctx,conn)
}

func (m *MasterNode) handleConnection(ctx context.Context, conn net.Conn) {
    defer conn.Close()
    decoder := gob.NewDecoder(conn)

    for {
        select {
        case <-ctx.Done():
            log.Printf("Context cancelled, closing connection")
            return
        default:
            var msg transport.Message
            log.Printf("MasterNode Attempting to decode...")
            decoder.Decode(&msg)

            log.Printf("Decoding Msg: %v", msg)
            go func() {
                opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
                defer cancel()

                if msg.Category == transport.MessageCategory_REQUEST {
                    switch msg.Operation {
                    case transport.MessageOperation_REGISTER:
                        log.Printf("Master Node Message parse")
                        payload, ok := msg.Payload.(*transport.RequestPayload)
                        log.Printf("Master Node Message parse ok")
                        if ok {
                            err := m.Register(opCtx, payload.Register)
                            if err != nil {
                                log.Printf("Register error: %v", err)
                            }
                        }
                    case transport.MessageOperation_UPLOAD:
                        payload, ok := msg.Payload.(*transport.RequestPayload)
                        if ok {
                            log.Printf("Upload Start : %v", payload)
                            err := m.UploadFile(opCtx, payload.Upload)
                            var response *transport.Message
                            if err != nil {
                                log.Printf("Upload error: %v", err)
                                response = &transport.Message{
                                    Category:  transport.MessageCategory_RESPONSE,
                                    Operation: transport.MessageOperation_UPLOAD,
                                    Payload: &transport.ResponsePayload{
                                        Upload: &transport.UploadFileResponse{
                                            Success: false,
                                            Message: err.Error(),
                                        },
                                    },
                                }
                            }else{
                                response = &transport.Message{
                                    Category:  transport.MessageCategory_RESPONSE,
                                    Operation: transport.MessageOperation_UPLOAD,
                                    Payload: &transport.ResponsePayload{
                                        Upload: &transport.UploadFileResponse{
                                            Success: true,
                                            Message: "File save success",
                                        },
                                    },
                                }
                            }
                            transport.SendMessage(conn, response)
                        }
                    }
                }

            }()
        }
    }
}


func (m *MasterNode) HasFile(ctx context.Context,userId, filename string) (bool) {
	return false
}

// node/master.go
func (m *MasterNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest) error {

	fileExists := m.HasFile(ctx,req.UserID, req.Filename)

	if fileExists {
		switch req.Policy {
		case policy.Overwrite:
			// return m.handleOverwrite(ctx, req, stream)
            return nil
		default:
			return fmt.Errorf("can't")
		}
	} else {
		return m.handleNewUpload(ctx, req)
	}
}

func (m *MasterNode) handleNewUpload(ctx context.Context, req *transport.UploadFileRequest) error {
    log.Printf("MasterNode: Handling new upload for file %s from user %s", req.Filename, req.UserID)

    chunks := utils.SplitFileIntoChunks(req.Content)
    log.Printf("MasterNode: File split into %d chunks", len(chunks))

    distribution := m.distributeChunks(chunks)

    var wg sync.WaitGroup
    errChan := make(chan error, len(distribution))

    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    for nodeID, nodeChunks := range distribution {
        wg.Add(1)
        go func(nodeID string, chunks []*transport.FileChunk) {
            defer wg.Done()
            if err := m.sendChunksToDataNode(ctx, nodeID, req.UserID, req.Filename, chunks); err != nil {
                errChan <- err
                cancel()  // 에러가 발생하면 모든 고루틴 취소
            }
        }(nodeID, nodeChunks)
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()

    for err := range errChan {
        if err != nil {
            log.Printf("MasterNode: Error during upload: %v", err)
            return err
        }
    }

    log.Printf("MasterNode: Upload completed for file %s", req.Filename)


    return nil
}

func (m *MasterNode) sendChunksToDataNode(ctx context.Context, nodeID, userID, filename string, chunks []*transport.FileChunk) error {
    log.Printf("Starting to send %d chunks to node %s for file %s", len(chunks), nodeID, filename)
    
    node, ok := m.dataNodes[nodeID]
    if !ok {
        return fmt.Errorf("DataNode %s not found", nodeID)
    }

    conn, err := net.DialTimeout("tcp", node.Addr, 15*time.Second)
    if err != nil {
        return fmt.Errorf("failed to connect to DataNode %s: %v", nodeID, err)
    }
    defer conn.Close()

    stream := transport.NewTCPStream(conn)

    for i, chunk := range chunks {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            log.Printf("Sending chunk %d/%d to node %s", i+1, len(chunks), nodeID)
            msg := &transport.Message{
                Category:  transport.MessageCategory_REQUEST,
                Operation: transport.MessageOperation_UPLOAD_CHUNK,
                Payload: &transport.RequestPayload{
                    UploadChunk: &transport.UploadFileChunkRequest{
                        UserID:   userID,
                        Filename: filename,
                        Chunk:    *chunk,
                    },
                },
            }
            err := stream.Send(msg)
            if err != nil {
                return fmt.Errorf("failed to send chunk %d: %v", i+1, err)
            }
        }
    }

    response, err := stream.CloseAndRecv()
    if err != nil {
        return fmt.Errorf("failed to receive response: %v", err)
    }

    if response.Category != transport.MessageCategory_RESPONSE || response.Operation != transport.MessageOperation_UPLOAD_CHUNK {
        return fmt.Errorf("unexpected response: %v", response)
    }

    payload, ok := response.Payload.(*transport.ResponsePayload)
    if !ok || payload.UploadChunk == nil {
        return fmt.Errorf("invalid response payload")
    }

    if !payload.UploadChunk.Success {
        return fmt.Errorf("upload failed: %s", payload.UploadChunk.Message)
    }

    log.Printf("Successfully uploaded all chunks to node %s", nodeID)
    return nil
}


func (m *MasterNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest) error {
	return fmt.Errorf("file not found: %s", req.Filename)
}

func (m *MasterNode) DeleteFile(ctx context.Context, req *transport.DeleteFileRequest) (*transport.DeleteFileResponse, error) {
	exists := m.HasFile(ctx,req.UserID, req.Filename)
	if !exists {
		return &transport.DeleteFileResponse{
			Success: false,
			Message: fmt.Sprintf("File %s not found", req.Filename),
		}, nil
	}

	return &transport.DeleteFileResponse{
        Success: true,
        Message: fmt.Sprintf("File %s successfully deleted", req.Filename),
    }, nil
}



func (m *MasterNode) ListFiles(ctx context.Context, req *transport.ListFilesRequest) (*transport.ListFilesResponse, error) {
	// 구현 로직
	return nil, nil
}

func (m *MasterNode) Start(ctx context.Context,addr string) error {
    transport, err := transport.NewMasterTCPTransport(addr, m)
    if err != nil {
        return fmt.Errorf("failed to set up TCP transport: %v", err)
    }
    m.transport = transport

    errChan := make(chan error, 1)
    go func() {
        errChan <- m.transport.Serve(ctx)
    }()

    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-m.stopChan:
        return nil
    case err := <-errChan:
        return err
    }
}

func (m *MasterNode) Stop() {
    close(m.stopChan)
    if m.transport != nil {
        m.transport.Close()
    }
}


func RunMasterNode(ctx context.Context, id, addr string) error {
    master := NewMasterNode(id)
    return master.Start(ctx, addr)
}

func (m *MasterNode) handleRegister(req *transport.RegisterMessage) error {
    log.Printf("MasterNode: Received registration request from %v\n", req)

    m.mu.Lock()
    defer m.mu.Unlock()

    _, err := net.Dial("tcp", req.Addr)
    if err != nil {
        return fmt.Errorf("failed to connect to DataNode %s: %v", req.NodeID, err)
    }

    m.dataNodes[req.NodeID] = &DataNodeInfo{
        ID:   req.NodeID,
        Addr: req.Addr,
        // Conn: conn,
    }

    log.Printf("Registered DataNode: %s at %s", req.NodeID, req.Addr)
    return nil
}

func (m *MasterNode) Register(ctx context.Context, req *transport.RegisterMessage) error {
    log.Printf("Register method")
    return m.handleRegister(req)
}

func (m *MasterNode) distributeChunks(chunks []*transport.FileChunk) map[string][]*transport.FileChunk {
    m.mu.RLock()
    defer m.mu.RUnlock()

    distribution := make(map[string][]*transport.FileChunk)
    nodeIDs := make([]string, 0, len(m.dataNodes))
    
    for nodeID := range m.dataNodes {
        nodeIDs = append(nodeIDs, nodeID)
    }

    if len(nodeIDs) == 0 {
        log.Println("No available data nodes for chunk distribution")
        return distribution
    }

    for i, chunk := range chunks {
        nodeIndex := i % len(nodeIDs)
        nodeID := nodeIDs[nodeIndex]
        distribution[nodeID] = append(distribution[nodeID], chunk)
    }

    // Log distribution information
    for nodeID, nodeChunks := range distribution {
        log.Printf("Node %s assigned %d chunks", nodeID, len(nodeChunks))
    }

    return distribution
}



func (m *MasterNode) GetConnectedDataNodesCount() int {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return len(m.dataNodes)
}
