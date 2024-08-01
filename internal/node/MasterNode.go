package node

// internal/node/MasterNode.go

import (
	policy "ChestyO/internal/enum"
	"ChestyO/internal/transport"
	"ChestyO/internal/utils"
	"context"
	"fmt"
	"log"
	"net"
	"sync"
)

type DataNodeInfo struct {
    ID      string
    Addr    string
    Conn    net.Conn
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

func (m *MasterNode) HasFile(ctx context.Context,userId, filename string) (bool) {
	return false
}

// node/master.go
func (m *MasterNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest,stream transport.UploadStream) error {

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
		return m.handleNewUpload(ctx, req,stream)
	}
}

func (m *MasterNode) handleNewUpload(ctx context.Context, req *transport.UploadFileRequest, stream transport.UploadStream) error {
    log.Printf("MasterNode: Handling new upload for file %s from user %s", req.Filename, req.UserID)

    // 파일을 청크로 분할
    chunks := utils.SplitFileIntoChunks(req.Content)
    log.Printf("MasterNode: File split into %d chunks", len(chunks))

    // 청크를 데이터 노드에 분배
    distribution := m.distributeChunks(chunks)

    // 각 데이터 노드에 청크 전송
    for nodeID, nodeChunks := range distribution {
        err := m.sendChunksToDataNode(ctx, nodeID, req.UserID, req.Filename, nodeChunks)
        if err != nil {
            log.Printf("Error sending chunks to node %s: %v", nodeID, err)
            return err
        }
    }

    // 파일 위치 정보 업데이트
    m.mu.Lock()
    m.fileLocations[req.Filename] = make([]string, 0, len(distribution))
    for nodeID := range distribution {
        m.fileLocations[req.Filename] = append(m.fileLocations[req.Filename], nodeID)
    }
    m.mu.Unlock()

    // 업로드 완료 응답
    err := stream.SendAndClose(&transport.UploadFileResponse{
        Success: true,
        Message: fmt.Sprintf("File %s uploaded successfully", req.Filename),
    })
    if err != nil {
        log.Printf("Error sending upload response: %v", err)
        return err
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

    stream := transport.NewTCPUploadStream(node.Conn)

    for i, chunk := range chunks {
        log.Printf("Sending chunk %d/%d to node %s", i+1, len(chunks), nodeID)
        err := stream.Send(&transport.UploadFileChunkRequest{
            UserID:   userID,
            Filename: filename,
            Chunk:    *chunk,
        })
        if err != nil {
            log.Printf("Error sending chunk %d to node %s: %v", i+1, nodeID, err)
            return fmt.Errorf("failed to send chunk %d: %v", i+1, err)
        }
        log.Printf("Successfully sent chunk %d/%d to node %s", i+1, len(chunks), nodeID)
    }

    log.Printf("All chunks sent to node %s, waiting for response", nodeID)
    response, err := stream.CloseAndRecv()
    if err != nil {
        log.Printf("Error receiving response from node %s: %v", nodeID, err)
        return fmt.Errorf("failed to receive response: %v", err)
    }

    if !response.Success {
        log.Printf("Upload failed for node %s: %s", nodeID, response.Message)
        return fmt.Errorf("upload failed: %s", response.Message)
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

    conn, err := net.Dial("tcp", req.Addr)
    if err != nil {
        return fmt.Errorf("failed to connect to DataNode %s: %v", req.NodeID, err)
    }

    m.dataNodes[req.NodeID] = &DataNodeInfo{
        ID:   req.NodeID,
        Addr: req.Addr,
        Conn: conn,
    }

    log.Printf("Registered DataNode: %s at %s", req.NodeID, req.Addr)
    return nil
}

func (m *MasterNode) Register(ctx context.Context, req *transport.RegisterMessage) error {
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
