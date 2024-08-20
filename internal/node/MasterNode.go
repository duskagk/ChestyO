package node

// internal/node/MasterNode.go

import (
	"ChestyO/internal/config"
	policy "ChestyO/internal/enum"
	"ChestyO/internal/kvclient"
	"ChestyO/internal/rest"
	"ChestyO/internal/transport"
	"ChestyO/internal/utils"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"sync"
	"time"
)

type DataNodeInfo struct {
    ID              string
    Addr            string
}

type MasterNode struct {
    ID              string
    dataNodes       map[string]*DataNodeInfo
    tcpTransport    *transport.TCPTransport
    restServer      *rest.RestServer
    stopChan        chan struct{}
    kvClient        *kvclient.KVClient
}

func NewMasterNode(id,tcpAddr, httpAddr string, bucknum int) *MasterNode {
    m := &MasterNode{
        ID:             id,
        dataNodes:      make(map[string]*DataNodeInfo),
        stopChan:       make(chan struct{}),
    }
    
    transport, err := transport.NewTCPTransport(tcpAddr, m)
    if err != nil {
        return nil
    }
    m.tcpTransport = transport
    
    // RestServer 생성
    m.restServer = rest.NewServer(m, httpAddr)

    cfg, err := config.LoadConfig("config.yaml")

    if err !=nil{
        return nil
    }

    m.kvClient = kvclient.NewKVClient(cfg.KVServer.Server.Host,cfg.KVServer.Server.Port)

    return m
}

func (m *MasterNode)TCPProtocl(ctx context.Context, conn net.Conn){
    log.Printf("Master Node : get Tcp Protocol")
    m.handleConnection(ctx,conn)
}

func (m *MasterNode) handleConnection(ctx context.Context, conn net.Conn) {
    // defer conn.Close()
    // decoder := gob.NewDecoder(conn)
    stream := transport.NewTCPStream(conn)
    // for {
        select {
        case <-ctx.Done():
            log.Printf("Context cancelled, closing connection")
            return
        default:
            var msg *transport.Message
            log.Printf("MasterNode Attempting to decode... %v",conn)
            // decoder.Decode(&msg)
            msg,_ = stream.Recv()

            log.Printf("Decoding Msg: %v", msg)
            go func() {

                opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
                defer cancel()

                if msg.Category == transport.MessageCategory_REQUEST {
                    payload, ok := msg.Payload.(*transport.RequestPayload)
                    switch msg.Operation {
                    case transport.MessageOperation_REGISTER:
                        if ok {
                            err := m.Register(opCtx, payload.Register)
                            if err != nil {
                                log.Printf("Register error: %v", err)
                            }
                            stream := transport.NewTCPStream(conn)
                            resp := &transport.Message{
                                Category: transport.MessageCategory_REQUEST,
                                Operation:  transport.MessageOperation_REGISTER,
                                Payload :  &transport.ResponsePayload{
                                    Register: &transport.RegisterResponse{
                                        BaseResponse: transport.BaseResponse{
                                            Success: true,
                                            Message: "Success Register",
                                        },
                                    },
                                },
                            }
                            log.Printf("Register success")
                            stream.Send(resp)
                        }
                    // case transport.MessageOperation_UPLOAD:
                    //     if ok {
                    //         log.Printf("Upload Start : %v", payload)
                    //         err := m.UploadFile(opCtx, payload.Upload)
                    //         var response *transport.Message
                    //         if err != nil {
                    //             log.Printf("Upload error: %v", err)
                    //             response = &transport.Message{
                    //                 Category:  transport.MessageCategory_RESPONSE,
                    //                 Operation: transport.MessageOperation_UPLOAD,
                    //                 Payload: &transport.ResponsePayload{
                    //                     Upload: &transport.UploadFileResponse{
                    //                         BaseResponse: transport.BaseResponse{
                    //                             Success: false,
                    //                             Message: err.Error(),
                    //                         },
                    //                     },
                    //                 },
                    //             }
                    //         }else{
                    //             response = &transport.Message{
                    //                 Category:  transport.MessageCategory_RESPONSE,
                    //                 Operation: transport.MessageOperation_UPLOAD,
                    //                 Payload: &transport.ResponsePayload{
                    //                     Upload: &transport.UploadFileResponse{
                    //                         BaseResponse: transport.BaseResponse{
                    //                             Success: true,
                    //                             Message: "File save success",
                    //                         },
                    //                     },
                    //                 },
                    //             }
                    //         }
                    //         transport.SendMessage(conn, response)
                    //     }
                    //     return
                    // case transport.MessageOperation_DOWNLOAD:
                    //     if ok{
                    //         file,err := m.DownloadFile(opCtx, payload.Download)
                    //         stream := transport.NewTCPStream(conn)
                    //         var response *transport.Message 
                    //         if err!=nil{
                    //             response = &transport.Message{
                    //                 Category:  transport.MessageCategory_RESPONSE,
                    //                 Operation: transport.MessageOperation_DOWNLOAD,
                    //                 Payload: &transport.ResponsePayload{
                    //                     Download: &transport.DownloadFileResponse{
                    //                         BaseResponse: transport.BaseResponse{
                    //                             Success: false,
                    //                             Message: err.Error(),
                    //                         },
                    //                         FileContent: file,
                    //                     },
                    //                 },
                    //             }
                    //         }else{
                    //             response = &transport.Message{
                    //                 Category:  transport.MessageCategory_RESPONSE,
                    //                 Operation: transport.MessageOperation_DOWNLOAD,
                    //                 Payload: &transport.ResponsePayload{
                    //                     Download: &transport.DownloadFileResponse{
                    //                         BaseResponse: transport.BaseResponse{
                    //                             Success: true,
                    //                             Message: "다운로드 성공",
                    //                         },
                    //                         FileContent: file,
                    //                     },
                    //                 },
                    //             }
                    //         }
                    //         stream.Send(response);
                    //     }
                    //     return
                    // case transport.MessageOperation_DELETE:
                    //     if ok{
                    //         err := m.DeleteFile(opCtx, payload.Delete)
                    //         if err!=nil{
                    //             response := &transport.Message{
                    //                 Category:  transport.MessageCategory_RESPONSE,
                    //                 Operation: transport.MessageOperation_DELETE,
                    //                 Payload: &transport.ResponsePayload{
                    //                     Delete: &transport.DeleteFileResponse{
                    //                         BaseResponse: transport.BaseResponse{
                    //                             Success: false,
                    //                             Message: err.Error(),
                    //                         },
                    //                     },
                    //                 },
                    //             }
                    //             transport.SendMessage(conn, response)
                    //         }else{
                    //             response := &transport.Message{
                    //                 Category:  transport.MessageCategory_RESPONSE,
                    //                 Operation: transport.MessageOperation_DELETE,
                    //                 Payload: &transport.ResponsePayload{
                    //                     Delete: &transport.DeleteFileResponse{
                    //                         BaseResponse: transport.BaseResponse{
                    //                             Success: true,
                    //                             Message: "Success Delete File",
                    //                         },
                    //                     },
                    //                 },
                    //             }
                    //             transport.SendMessage(conn, response)
                    //         }
                    //         return
                    //     }
                    default:
                    }
                }

            }()
        }
    // }
}


func (m *MasterNode) HasFile(ctx context.Context, userID, filename string) (bool, map[string]bool) {
    responses := make(map[string]bool)
    var mu sync.Mutex
    var wg sync.WaitGroup

    for nodeID, node := range m.dataNodes {
        wg.Add(1)
        go func(nodeID, addr string) {
            defer wg.Done()
            hasFile, err := m.checkFileInDataNode( addr, userID, filename)
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

func (m *MasterNode) checkFileInDataNode(addr, user_id, filename string) (bool, error) {
    log.Printf("MasterNode: check for file %s from user %s", filename, user_id)

    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return false, err
    }
    defer conn.Close()  // 연결 종료 추가

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
    if err := stream.Send(request); err != nil {
        return false, err
    }
    msg, err := stream.CloseAndRecv()
    if err != nil {
        return false, err
    }
    payload, ok := msg.Payload.(*transport.ResponsePayload)

    if !ok || payload.HasFile == nil {
        return false, fmt.Errorf("error correct response")
    }

    return payload.HasFile.IsExist, nil
}

// node/master.go
func (m *MasterNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest) error {

	fileExists,_ := m.HasFile(ctx,req.UserID, req.Filename)

    log.Printf("File exist %v",fileExists)

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


    metadata := kvclient.FileMetadata{
        RetentionTime: time.Now().AddDate(0, 0, 90),  // 지금으로부터 90일 뒤
        FileSize:      int64(len(req.Content)),
        ChunkNodes:    []string{"node1"},
    }
    key := fmt.Sprintf("file:%s:%s", req.UserID, req.Filename)
    metadataJSON, err := json.Marshal(metadata)

    if err !=nil{
        return err
    }
    // 현재 시간
    m.kvClient.Set(key, string(metadataJSON))

    return nil
}


func (m *MasterNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest) ([]byte,error) {
    log.Printf("MasterNode: Starting download for file %s from user %s", req.Filename, req.UserID)

    // stream := transport.NewTCPStream(conn)

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
            return nil,fmt.Errorf("error during download: %v", err)
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

    log.Printf("MasterNode: Successfully downloaded and sent file %s for user %s", req.Filename, req.UserID)
    return fullFileContent,nil
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
            // log.Printf("Receive Data %v", &payload.DownloadChunk.Chunk)
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


func (m *MasterNode) DeleteFile(ctx context.Context, req *transport.DeleteFileRequest)  error{
	exists,nodes := m.HasFile(ctx,req.UserID, req.Filename)

    if !exists {
        return fmt.Errorf("file %s does not exist for user %s", req.Filename, req.UserID)
    }

    var wg sync.WaitGroup
    errChan := make(chan error, len(nodes))

    for nodeID, hasFile := range nodes {
        if !hasFile {
            continue
        }

        wg.Add(1)
        go func(nodeID string) {
            defer wg.Done()
            node, ok := m.dataNodes[nodeID]
            if !ok {
                errChan <- fmt.Errorf("data node %s not found", nodeID)
                return
            }

            err := m.deleteFileFromDataNode(ctx, node.Addr, req.UserID, req.Filename)
            if err != nil {
                errChan <- fmt.Errorf("failed to delete file from node %s: %v", nodeID, err)
            }
        }(nodeID)
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()

    var errs []error
    for err := range errChan {
        if err != nil {
            errs = append(errs, err)
        }
    }

    if len(errs) > 0 {
        return fmt.Errorf("errors occurred during file deletion: %v", errs)
    }

    log.Printf("MasterNode: Successfully deleted file %s for user %s", req.Filename, req.UserID)
    return nil
}


func (m *MasterNode) deleteFileFromDataNode(ctx context.Context, addr, userID, filename string) error {
    conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
    if err != nil {
        return fmt.Errorf("failed to connect to data node: %v", err)
    }
    defer conn.Close()

    stream := transport.NewTCPStream(conn)

    request := &transport.Message{
        Category:  transport.MessageCategory_REQUEST,
        Operation: transport.MessageOperation_DELETE,
        Payload: &transport.RequestPayload{
            Delete: &transport.DeleteFileRequest{
                UserID:   userID,
                Filename: filename,
            },
        },
    }

    log.Printf("Send delete file request")

    if err := stream.Send(request); err != nil {
        return fmt.Errorf("failed to send delete request: %v", err)
    }

    response, err := stream.CloseAndRecv()
    if err != nil {
        return fmt.Errorf("failed to receive delete response: %v", err)
    }

    payload, ok := response.Payload.(*transport.ResponsePayload)
    if !ok || payload.Delete == nil {
        return fmt.Errorf("invalid response from data node")
    }

    if !payload.Delete.Success {
        return fmt.Errorf("data node failed to delete file: %s", payload.Delete.Message)
    }

    return nil
}


func (m *MasterNode) ListFiles(ctx context.Context, req *transport.ListFilesRequest) (*transport.ListFilesResponse, error) {
	// 구현 로직
	return nil, nil
}

func (m *MasterNode) Start(ctx context.Context) error {
    errChan := make(chan error, 1)
    go func() {
        errChan <- m.tcpTransport.Serve(ctx)
    }()

    go func(){
        if err := m.restServer.Serve(ctx); err !=nil{
            log.Printf("REST server error: %v", err)
            errChan <- err
        }
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
    if m.tcpTransport != nil {
        m.tcpTransport.Close()
    }
}


func (m *MasterNode) handleRegister(req *transport.RegisterMessage) error {
    log.Printf("MasterNode: Received registration request from %v\n", req)
    nodeInfo := &DataNodeInfo{
        ID:   req.NodeID,
        Addr: req.Addr,
    }

    m.dataNodes[req.NodeID] = nodeInfo


    log.Printf("Registered DataNode: %s at %s", req.NodeID, req.Addr)
    return nil
}

func (m *MasterNode) Register(ctx context.Context, req *transport.RegisterMessage) error {
    log.Printf("Register method")
    return m.handleRegister(req)
}

func (m *MasterNode) distributeChunks(chunks []*transport.FileChunk) map[string][]*transport.FileChunk {
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
    return len(m.dataNodes)
}



