package node

// internal/node/MasterNode.go

import (
	policy "ChestyO/internal/enum"
	"ChestyO/internal/transport"
	"ChestyO/internal/utils"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
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
                        payload, ok := msg.Payload.(*transport.RequestPayload)
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
                                            BaseResponse: transport.BaseResponse{
                                                Success: false,
                                                Message: err.Error(),
                                            },
                                        },
                                    },
                                }
                            }else{
                                response = &transport.Message{
                                    Category:  transport.MessageCategory_RESPONSE,
                                    Operation: transport.MessageOperation_UPLOAD,
                                    Payload: &transport.ResponsePayload{
                                        Upload: &transport.UploadFileResponse{
                                            BaseResponse: transport.BaseResponse{
                                                Success: true,
                                                Message: "File save success",
                                            },
                                        },
                                    },
                                }
                            }
                            transport.SendMessage(conn, response)
                        }
                    case transport.MessageOperation_DOWNLOAD:
                        payload, ok := msg.Payload.(*transport.RequestPayload)
                        if ok{
                            m.DownloadFile(opCtx, payload.Download, conn)
                        }
                    default:
                    }
                }

            }()
        }
    }
}


func (m *MasterNode) HasFile(ctx context.Context, userID, filename string) (bool, map[string]bool) {
    responses := make(map[string]bool)
    var mu sync.Mutex
    var wg sync.WaitGroup

    for nodeID, node := range m.dataNodes {
        wg.Add(1)
        go func(nodeID, addr string) {
            defer wg.Done()
            hasFile, err := m.checkFileInDataNode(ctx, addr, userID, filename)
            if err != nil {
                log.Printf("Error checking file in node %s: %v", nodeID, err)
                return
            }
            mu.Lock()
            responses[nodeID] = hasFile
            mu.Unlock()
        }(nodeID, node.Addr)
    }

    wg.Wait()

    hasFile := false
    for _, exists := range responses {
        if exists {
            hasFile = true
            break
        }
    }

    return hasFile, responses
}

func (m *MasterNode) checkFileInDataNode(ctx context.Context,addr,user_id, filename string) (bool,error){
    log.Printf("MasterNode: check for file %s from user %s",filename, user_id)
    ctx, cancel := context.WithCancel(ctx)
    conn, err := net.Dial("tcp", addr)
    defer cancel()
    

    if err!=nil{
        return false, err
    }

    stream := transport.NewTCPStream(conn)

    request := &transport.Message{
        Category:  transport.MessageCategory_REQUEST,
        Operation: transport.MessageOperation_HASFILE,
        Payload: &transport.RequestPayload{
            HasFile: &transport.HasFileRequest{
                UserID:   user_id,
                Filename: filename,
            },
        },
    }
    if err :=stream.Send(request); err!=nil{
        return false, err
    }
    msg,err := stream.Recv()
    payload, ok := msg.Payload.(*transport.ResponsePayload)

    if !ok{
        return false, fmt.Errorf("Not correct response")
    }

    return payload.HasFile.IsExist, nil
}

// node/master.go
func (m *MasterNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest) error {

	fileExists,_ := m.HasFile(ctx,req.UserID, req.Filename)

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


func (m *MasterNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest, conn net.Conn) error {
    log.Printf("MasterNode: Starting download for file %s from user %s", req.Filename, req.UserID)

    stream := transport.NewTCPStream(conn)

    // 모든 DataNode에 다운로드 요청 전송
    var wg sync.WaitGroup
    errChan := make(chan error, len(m.dataNodes))
    chunksChan := make(chan *transport.FileChunk, 100*100) // 버퍼 크기는 적절히 조정

    for _, node := range m.dataNodes {
        wg.Add(1)
        go func(nodeID, addr string) {
            defer wg.Done()
            err := m.requestChunksFromDataNode(ctx, nodeID, addr, req.UserID, req.Filename, chunksChan)
            if err != nil {
                errChan <- err
            }
        }(node.ID, node.Addr)
    }

    // 청크 수집
    go func() {
        wg.Wait()
        close(chunksChan)
        close(errChan)
    }()

    // 청크를 수집하여 완전한 파일로 만들기
    chunks := make(map[int][]byte)
    for chunk := range chunksChan {
        chunks[chunk.Index] = chunk.Content
    }

    // 에러 확인
    for err := range errChan {
        if err != nil {
            return fmt.Errorf("error during download: %v", err)
        }
    }

    // 청크를 정렬하고 합치기
    var sortedIndices []int
    for index := range chunks {
        sortedIndices = append(sortedIndices, int(index))
    }
    sort.Ints(sortedIndices)

    var fullFileContent []byte
    for _, index := range sortedIndices {
        fullFileContent = append(fullFileContent, chunks[int(index)]...)
    }

    // 클라이언트에게 전체 파일 전송
    response := &transport.Message{
        Category:  transport.MessageCategory_RESPONSE,
        Operation: transport.MessageOperation_DOWNLOAD,
        Payload: &transport.ResponsePayload{
            Download: &transport.DownloadFileResponse{
                BaseResponse: transport.BaseResponse{
                    Success: true,
                    Message: "다운로드 성공",
                },
                FileContent: fullFileContent,
            },
        },
    }
    if err := stream.Send(response); err != nil {
        return fmt.Errorf("failed to send file to client: %v", err)
    }

    log.Printf("MasterNode: Successfully downloaded and sent file %s for user %s", req.Filename, req.UserID)
    return nil
}

func (m *MasterNode) requestChunksFromDataNode(ctx context.Context, nodeID, addr, userID, filename string, chunksChan chan<- *transport.FileChunk) error {
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return fmt.Errorf("failed to connect to DataNode %s: %v", nodeID, err)
    }
    defer conn.Close()

    stream := transport.NewTCPStream(conn)

    request := &transport.Message{
        Category:  transport.MessageCategory_REQUEST,
        Operation: transport.MessageOperation_DOWNLOAD_CHUNK,
        Payload: &transport.RequestPayload{
            DownLoadChunk: &transport.DownloadChunkRequest{
                UserID:   userID,
                Filename: filename,
            },
        },
    }

    if err := stream.Send(request); err != nil {
        return fmt.Errorf("failed to send download request to DataNode %s: %v", nodeID, err)
    }

    log.Printf("Start Get Data %s", nodeID)
    for {
        response, err := stream.Recv()
        if err == io.EOF {
            log.Printf("The stream from DataNode %s is end", nodeID)
            break
        }
        if err != nil {
            return fmt.Errorf("error receiving chunk from DataNode %s: %v", nodeID, err)
        }

        if response.Category == transport.MessageCategory_RESPONSE && response.Operation == transport.MessageOperation_DOWNLOAD_CHUNK {
            payload, ok := response.Payload.(*transport.ResponsePayload)
            if !ok || payload.DownloadChunk == nil {
                return fmt.Errorf("invalid payload for download chunk from DataNode %s", nodeID)
            }
            if payload.DownloadChunk.Success && payload.DownloadChunk.Message == "All chunks sent" {
                log.Printf("All chunks received from DataNode %s", nodeID)
                break
            }
            log.Printf("Receive Data %v", &payload.DownloadChunk.Chunk)
            chunksChan <- &payload.DownloadChunk.Chunk
        }
    }

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

    var wg sync.WaitGroup
    errChan := make(chan error, len(chunks))

    for i, chunk := range chunks {
        wg.Add(1)
        go func(i int, chunk *transport.FileChunk) {
            defer wg.Done()
            select {
            case <-ctx.Done():
                errChan <- ctx.Err()
                return
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
                    errChan <- fmt.Errorf("failed to send chunk %d: %v", i+1, err)
                    return
                }

                // 각 청크 전송 후 응답 대기
                response, err := stream.Recv()
                if err != nil {
                    errChan <- fmt.Errorf("failed to receive response for chunk %d: %v", i+1, err)
                    return
                }

                if response.Category != transport.MessageCategory_RESPONSE || response.Operation != transport.MessageOperation_UPLOAD_CHUNK {
                    errChan <- fmt.Errorf("unexpected response for chunk %d: %v", i+1, response)
                    return
                }

                payload, ok := response.Payload.(*transport.ResponsePayload)
                if !ok || payload.UploadChunk == nil {
                    errChan <- fmt.Errorf("invalid response payload for chunk %d", i+1)
                    return
                }

                if !payload.UploadChunk.Success {
                    errChan <- fmt.Errorf("upload failed for chunk %d: %s", i+1, payload.UploadChunk.Message)
                    return
                }
            }
        }(i, chunk)
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()

    for err := range errChan {
        if err != nil {
            return err
        }
    }

    log.Printf("Successfully uploaded all chunks to node %s", nodeID)
    return nil
}


func (m *MasterNode) DeleteFile(ctx context.Context, req *transport.DeleteFileRequest) (*transport.DeleteFileResponse, error) {
	exists,_ := m.HasFile(ctx,req.UserID, req.Filename)
	if !exists {
		return &transport.DeleteFileResponse{
            BaseResponse: transport.BaseResponse{
                Success: false,
                Message: fmt.Sprintf("File %s not found", req.Filename),
            },
		}, nil
	}

	return &transport.DeleteFileResponse{
        BaseResponse: transport.BaseResponse{
            Success: true,
            Message: fmt.Sprintf("File %s successfully deleted", req.Filename),
        },
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




