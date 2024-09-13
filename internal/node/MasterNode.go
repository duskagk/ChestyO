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
	"strings"
	"sync"
	"time"
)




type MasterNode struct {
    ID              string
    dataNodes       map[string]*DataNodeInfo
    tcpTransport    *transport.TCPTransport
    restServer      *rest.GinServer
    stopChan        chan struct{}
    kvServer        *kvclient.KVClient
    nodeMu          sync.Mutex
    nodeKeys        []string                 // 맵의 키를 저장할 리스트
	currentIndex    int                      // 현재 라운드 로빈 인덱스
}


type StreamNodePair struct {
    Stream *transport.StreamService
    Node   *DataNodeInfo
}

func NewMasterNode(tcpAddr, httpAddr string) *MasterNode {
    m := &MasterNode{
        // ID:             id,
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
        log.Fatal("Failed to load config.yaml")
        return nil
    }

    m.kvServer = kvclient.NewKVClient(cfg.KVServer.Server.Host,cfg.KVServer.Server.Port)

    return m
}

func (m *MasterNode)TCPProtocl(ctx context.Context, conn net.Conn){
    log.Printf("Master Node : get Tcp Protocol")
    m.handleConnection(ctx,conn)
}

func (m *MasterNode) handleConnection(ctx context.Context, conn net.Conn) {
    stream := transport.NewTCPStream(conn)

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


func (m *MasterNode) HasFile(ctx context.Context, userID, filename string) (bool, *transport.FileMetadata, error) {
    key := fmt.Sprintf("file:%s:%s", userID, filename)
    metadatabyte, err := m.kvServer.Get(key)

    if err != nil {
        return false, nil, fmt.Errorf("failed to get file metadata: %v", err)
    }

    if len(metadatabyte) == 0 {
        return false, nil, nil
    }

    var metadata transport.FileMetadata
    err = json.Unmarshal(metadatabyte, &metadata)
    if err != nil {
        return false, nil, fmt.Errorf("failed to unmarshal file metadata: %v", err)
    }

    return true, &metadata, nil
}



// node/master.go
func (m *MasterNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest) error {

	fileExists,_,err := m.HasFile(ctx,req.UserID, req.Filename)

    if err !=nil{
        return fmt.Errorf("error: Can not connect KVS server")
    }

    log.Printf("File exist %v",fileExists)

	if fileExists {
		switch req.Policy {
		case policy.Overwrite:
			// return m.handleOverwrite(ctx, req, stream)
            return nil
		default:
			return fmt.Errorf("error: Already Exist File")
		}
	} else {
		return m.handleNewUpload(ctx, req)
	}
}

func (m *MasterNode) handleNewUpload(ctx context.Context, req *transport.UploadFileRequest) error {
    log.Printf("MasterNode: Handling new upload for file %s from user %s", req.Filename, req.UserID)

    chunks := utils.SplitFileIntoChunks(req.Content)
    log.Printf("MasterNode: File split into %d chunks", len(chunks))

    fileMetadata := &transport.FileMetadata{
        FileSize:    int64(len(req.Content)),
        ContentType: req.FileContentType,
        TotalChunks: int64(len(chunks)),
        ChunkSize:   utils.ChunkSize,
    }

    if req.RetentionPeriodDays > 0 {
        fileMetadata.RetentionTime = time.Now().AddDate(0, 0, req.RetentionPeriodDays)
    } else {
        fileMetadata.RetentionTime = time.Now().AddDate(0, 0, 90)
    }

    replica := 2 // 복제본 수
    node_list,err := m.uploadChunksToNodes(ctx, req, chunks, replica);
    // 청크 업로드
    if err != nil {
        return fmt.Errorf("failed to upload chunks: %v", err)
    }


    fileMetadata.ChunkNodes = node_list

    // 파일 메타데이터 저장
    fileKey := fmt.Sprintf("file:%s:%s", req.UserID, req.Filename)
    if err := m.kvServer.Set(fileKey, fileMetadata); err != nil {
        return fmt.Errorf("failed to save file metadata: %v", err)
    }

    // 삭제 키 설정
    if !fileMetadata.RetentionTime.IsZero() {
        deleteKey := fmt.Sprintf("delete_file:%s:%s:%s",
            fileMetadata.RetentionTime.Format("20060102150405"),
            req.UserID,
            req.Filename)
        if err := m.kvServer.Set(deleteKey, ""); err != nil {
            log.Printf("Failed to set delete key: %v", err)
        }
    }

    // 버킷 메타데이터 업데이트
    if err := m.updateBucketMetadata(ctx, req.UserID, fileMetadata.FileSize); err != nil {
        log.Printf("Failed to update bucket metadata: %v", err)
    }

    log.Printf("MasterNode: Upload completed for file %s", req.Filename)
    return nil
}


func (m *MasterNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest) ([]*transport.FileChunk, error) {
    log.Printf("MasterNode: Starting download for file %s from user %s, range: %d-%d", req.Filename, req.UserID, req.Start, req.End)

    hasFile, metadata, err := m.HasFile(ctx, req.UserID, req.Filename)
    if err != nil {
        return nil, fmt.Errorf("error checking file existence: %v", err)
    }
    if !hasFile {
        return nil, fmt.Errorf("file not found: %s", req.Filename)
    }

    startChunk := req.Start / metadata.ChunkSize
    endChunk := (req.End + metadata.ChunkSize - 1) / metadata.ChunkSize // Round up to include partial end chunk

    prefix := fmt.Sprintf("chunk:%s:%s", req.UserID, req.Filename)
    cursor := fmt.Sprintf("%s:%05d", prefix, startChunk)

    chunkMetadata, _, err := m.kvServer.ScanValueByKey(prefix, cursor, int(endChunk-startChunk))
    if err != nil {
        return nil, fmt.Errorf("error fetching chunk metadata: %v", err)
    }

    log.Printf("Get Chunk meta data : %v", chunkMetadata)
    chunksChan := make(chan *transport.FileChunk, len(chunkMetadata))
    errChan := make(chan error, len(chunkMetadata))
    var wg sync.WaitGroup

    for _, chunkData := range chunkMetadata {
        wg.Add(1)
        go func(chunkData map[string]interface{}) {
            defer wg.Done()

            var chunkMeta transport.ChunkMetadata
            valueJSON, err := json.Marshal(chunkData["value"])
            if err != nil {
                log.Printf("Error marshaling chunk metadata: %v", err)
                errChan <- err
                return
            }
            
            if err := json.Unmarshal(valueJSON, &chunkMeta); err != nil {
                log.Printf("Error unmarshaling chunk metadata: %v", err)
                errChan <- err
                return
            }

            res_chunk, err := m.requestChunkStream(ctx, chunkMeta.NodeID[0], chunkMeta.ChunkIndex, req)
            if err != nil {
                log.Printf("Error requesting chunk %d: %v", chunkMeta.ChunkIndex, err)
                errChan <- err
                return
            }
            
            if chunkMeta.ChunkIndex == int(startChunk) {
                startOffset := req.Start % metadata.ChunkSize
                res_chunk.Content = res_chunk.Content[startOffset:]
            }
            if chunkMeta.ChunkIndex == int(endChunk-1) {
                endOffset := (req.End % metadata.ChunkSize) + 1
                if endOffset < int64(len(res_chunk.Content)) {
                    res_chunk.Content = res_chunk.Content[:endOffset]
                }
            }

            chunksChan <- res_chunk
            
            log.Printf("Processed chunk: Index %v, Length %v", res_chunk.Index, len(res_chunk.Content))

        }(chunkData)
    }

    go func() {
        wg.Wait()
        close(chunksChan)
        close(errChan)
    }()

    var chunks []*transport.FileChunk
    var processError error
    for i := 0; i < len(chunkMetadata); i++ {
        select {
        case chunk, ok := <-chunksChan:
            if !ok {
                continue
            }
            chunks = append(chunks, chunk)
            req.Length += int64(len(chunk.Content))
        case err, ok := <-errChan:
            if !ok {
                continue
            }
            if err != nil {
                processError = err
                log.Printf("Error processing chunk: %v", err)
            }
        }
    }

    if processError != nil {
        return nil, fmt.Errorf("error processing chunks: %v", processError)
    }

    if len(chunks) == 0 {
        return nil, fmt.Errorf("no chunks were processed successfully")
    }

    sort.Slice(chunks, func(i, j int) bool {
        return chunks[i].Index < chunks[j].Index
    })

    log.Printf("Download completed for file %s, total chunks: %d, total length: %d", req.Filename, len(chunks), req.Length)

    return chunks, nil
}

func (m *MasterNode) requestChunkStream(ctx context.Context, nodeID string, chunkIndex int, req *transport.DownloadFileRequest) (*transport.FileChunk, error) {
    // 컨텍스트에 타임아웃 추가 (예: 30초)
    ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()

    // 컨텍스트를 사용하여 연결 시도
    var d net.Dialer
    conn, err := d.DialContext(ctx, "tcp", m.dataNodes[nodeID].Addr)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to DataNode %s: %v", nodeID, err)
    }
    defer conn.Close()

    stream := transport.NewTCPStream(conn)
    request := &transport.Message{
        Category:  transport.MessageCategory_REQUEST,
        Operation: transport.MessageOperation_DOWNLOAD_CHUNK,
        Payload: &transport.RequestPayload{
            DownLoadChunk: &transport.DownloadChunkRequest{
                UserID:     req.UserID,
                Filename:   req.Filename,
                ChunkIndex: chunkIndex,
            },
        },
    }

    // 요청 전송
    errChan := make(chan error, 1)
    go func() {
        errChan <- stream.Send(request)
    }()

    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    case err := <-errChan:
        if err != nil {
            return nil, fmt.Errorf("failed to send download request to DataNode %s: %v", nodeID, err)
        }
    }

    // 응답 수신
    responseChan := make(chan *transport.Message, 1)
    errorChan := make(chan error, 1)
    go func() {
        response, err := stream.Recv()
        if err != nil {
            errorChan <- err
            return
        }
        responseChan <- response
    }()

    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    case err := <-errorChan:
        if err == io.EOF {
            return nil, fmt.Errorf("the stream from DataNode %s is end", nodeID)
        }
        return nil, fmt.Errorf("error receiving chunk from DataNode %s: %v", nodeID, err)
    case response := <-responseChan:
        if response.Category == transport.MessageCategory_RESPONSE && response.Operation == transport.MessageOperation_DOWNLOAD_CHUNK {
            payload, ok := response.Payload.(*transport.ResponsePayload)
            if !ok || payload.DownloadChunk == nil {
                return nil, fmt.Errorf("invalid payload for download chunk from DataNode %s", nodeID)
            }
            if payload.DownloadChunk.Success && payload.DownloadChunk.Message == "All chunks sent" {
                log.Printf("All chunks received from DataNode %s", nodeID)
            }
            return &payload.DownloadChunk.Chunk, nil
        }
        return nil, fmt.Errorf("no Data request")
    }
}


func (m *MasterNode) sendChunkToDataNode(ctx context.Context, stream *transport.StreamService, userID, filename string, chunk *transport.FileChunk) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
        // log.Printf("Sending chunk %d for file %s", chunk.Index, filename)

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
            return fmt.Errorf("failed to send chunk %d: %v", chunk.Index, err)
        }

        // 청크 전송 후 응답 대기
        response, err := stream.Recv()
        if err != nil {
            return fmt.Errorf("failed to receive response for chunk %d: %v", chunk.Index, err)
        }

        if response.Category != transport.MessageCategory_RESPONSE || response.Operation != transport.MessageOperation_UPLOAD_CHUNK {
            return fmt.Errorf("unexpected response for chunk %d: %v", chunk.Index, response)
        }

        payload, ok := response.Payload.(*transport.ResponsePayload)
        if !ok || payload.UploadChunk == nil {
            return fmt.Errorf("invalid response payload for chunk %d", chunk.Index)
        }

        if !payload.UploadChunk.Success {
            return fmt.Errorf("upload failed for chunk %d: %s", chunk.Index, payload.UploadChunk.Message)
        }

        // log.Printf("Successfully uploaded chunk %d for file %s", chunk.Index, filename)
        return nil
    }
}




func (m *MasterNode) DeleteFile(ctx context.Context, req *transport.DeleteFileRequest)  error{
    log.Printf("MasterNode Delete bucket %v file %v", req.UserID, req.Filename)
	exists,metaData,err := m.HasFile(ctx,req.UserID, req.Filename)
    if !exists {
        return fmt.Errorf("MasterNode Hasfile fail : %v", err)
    }

    var wg sync.WaitGroup
    errChan := make(chan string, len(metaData.ChunkNodes))

    var node_err_list []string
    
    for _,nodeID := range metaData.ChunkNodes {
        wg.Add(1)
        go func(nodeID string) {
            defer wg.Done()
            node, ok := m.dataNodes[nodeID]
            log.Printf("Node not register :%v , is ok : %v", node,ok)
            if !ok {
                errChan <- nodeID
                return
            }

            conn, err := net.Dial("tcp", node.Addr)
            if err !=nil{
                log.Printf("Dial Datanode failed : %v", err)
                errChan <- nodeID
                return
            }
            stream := transport.NewTCPStream(conn)
            defer stream.CloseStream()
            err = m.deleteFileFromDataNode(ctx, stream, req.UserID, req.Filename)
            if err != nil {
                errChan <- node.ID
                node_err_list = append(node_err_list, nodeID)
            }
        }(nodeID)
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()


    metaKey := fmt.Sprintf("file:%s:%s", req.UserID, req.Filename)
    err = m.kvServer.Delete(metaKey);

    if err!=nil{
        return fmt.Errorf("error delete metadata : %v", err)
    }

    if !metaData.RetentionTime.IsZero(){
        delete_key := fmt.Sprintf("delete_file:%s:%s:%s",
        metaData.RetentionTime.Format("20060102150405"),
        req.UserID,
        req.Filename)
        m.kvServer.Delete(delete_key)
    }

    batch := kvclient.NewKVBatch()
    prefix := fmt.Sprintf("chunk:%s:%s", req.UserID, req.Filename)
    cursor := fmt.Sprintf("%s:%05d", prefix, 0)

    for {
        result, err := m.kvServer.ScanKey(prefix, cursor, 1000)
        if err != nil{
            return fmt.Errorf("error when scan key : %v",err)
        }

        for _,key := range result.Keys{
            batch.Add("delete",key,nil)
        }

        if result.NextCursor==""{
            break
        }
        cursor = result.NextCursor
    }

    for err_node :=range errChan {
        key := fmt.Sprintf("error_delete:%s:%s:%s",err_node,req.UserID,req.Filename)
        batch.Add("set",key,"")
    }

    err = m.kvServer.BatchOperation(batch.GetPairs());

    if err!=nil{
        return fmt.Errorf("error: Batch operation error : %v", err)
    }

    log.Printf("MasterNode: Successfully deleted file %s for user %s", req.Filename, req.UserID)
    return nil
}

func (m *MasterNode) deleteFileFromDataNode(ctx context.Context, stream *transport.StreamService, userID, filename string) error {
    
    select {
    case <- ctx.Done():
        return fmt.Errorf("delete Tiem out")
    default:
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
            log.Printf("Is error : %v", err)
            return fmt.Errorf("failed to send delete request: %v", err)
        }
    
        response, err := stream.CloseAndRecv()
        // response, err := stream.Recv()
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
}


func (m *MasterNode) GetFileList(ctx context.Context, req *transport.FileListRequest) (*transport.ListFilesResponse, error) {
    ret := &transport.ListFilesResponse{}
    key := fmt.Sprintf("file:%s:",req.Bucket)
    cursor,err := m.kvServer.ScanOffset(key,req.Offset)
    if err!=nil{
        ret.Message = err.Error()
        ret.Success = false
        return ret, err
    }
    Scanres,err := m.kvServer.ScanKey(key,cursor, req.Limit)
    if err !=nil{
        ret.BaseResponse.Message = err.Error()
        ret.BaseResponse.Success = false
        return ret, err
    }

    var files []string

    for _,key := range Scanres.Keys{
        sp_str := strings.Split(key, ":")
        if len(sp_str)==3{
            files = append(files,sp_str[2])
        }
    }

    ret.Files = files
    ret.Success = true

	return ret, nil
}

func (m *MasterNode) GetBuckets(ctx context.Context,limit, offset int)([]string,error){
    var ret []string

    cursor,_ := m.kvServer.ScanOffset("bucket:",offset)
    log.Printf("Key Data %d %s,%v",offset, cursor, limit)
    Scanres,err := m.kvServer.ScanKey("bucket:",cursor, limit)
    if err !=nil{
        return ret, err
    }
    
    for _,key := range Scanres.Keys{
        _spl := strings.Split(key, ":")
        ret = append(ret, _spl[1])
    }
    
    
    return ret,nil
}

func (m *MasterNode) Start(ctx context.Context) error {
    errChan := make(chan error, 1)
    go func() {
        errChan <- m.tcpTransport.Serve(ctx)
    }()

    go m.startDeleteTimer()

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
    m.nodeKeys = append(m.nodeKeys, req.NodeID)

    log.Printf("Registered DataNode: %s at %s", req.NodeID, req.Addr)
    return nil
}





func (m *MasterNode) Register(ctx context.Context, req *transport.RegisterMessage) error {
    log.Printf("Register method")
    return m.handleRegister(req)
}

func (m *MasterNode) getStream(streamNum int) []StreamNodePair {
    m.nodeMu.Lock()
    nodeKeys := make([]string, len(m.nodeKeys))
    copy(nodeKeys, m.nodeKeys)
    currentIndex := m.currentIndex
    m.nodeMu.Unlock()

    selectedPairs := make([]StreamNodePair, 0, streamNum)
    if len(nodeKeys) == 0 {
        return nil
    }

    tmpConList := make(map[string]bool, streamNum)
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
    defer cancel()

    startIndex := currentIndex
    for len(selectedPairs) < streamNum {
        select {
        case <-ctx.Done():
            return selectedPairs
        default:
            if currentIndex >= len(nodeKeys) {
                currentIndex = 0
            }

            nodeKey := nodeKeys[currentIndex]
            if tmpConList[nodeKey] || (currentIndex == startIndex && len(selectedPairs) > 0) {
                break
            }

            tmpConList[nodeKey] = true
            
            m.nodeMu.Lock()
            node, exists := m.dataNodes[nodeKey]
            m.nodeMu.Unlock()

            if !exists {
                currentIndex = (currentIndex + 1) % len(nodeKeys)
                continue
            }
            
            if stream := m.dialAndCreateStream(node); stream != nil {
                selectedPairs = append(selectedPairs, StreamNodePair{Stream: stream, Node: node})
            } else {
                go m.removeErrorNode(node.ID)
            }

            currentIndex = (currentIndex + 1) % len(nodeKeys)
        }
    }

    m.nodeMu.Lock()
    m.currentIndex = currentIndex
    m.nodeMu.Unlock()

    return selectedPairs
}

func (m *MasterNode) removeErrorNode(nodeID string) {
    m.deleteDataNode(nodeID)
    log.Printf("Removed error node: %s", nodeID)
}

func (m *MasterNode) deleteDataNode(nodeID string) {
    m.nodeMu.Lock()
    defer m.nodeMu.Unlock()

    if _, exists := m.dataNodes[nodeID]; !exists {
        return
    }

    delete(m.dataNodes, nodeID)
    for i, key := range m.nodeKeys {
        if key == nodeID {
            m.nodeKeys = append(m.nodeKeys[:i], m.nodeKeys[i+1:]...)
            break
        }
    }

    if m.currentIndex >= len(m.nodeKeys) {
        m.currentIndex = 0
    }
}


func (m *MasterNode) dialAndCreateStream(node *DataNodeInfo) *transport.StreamService {
    conn, err := net.DialTimeout("tcp", node.Addr, time.Second*3)
    if err != nil {
        log.Printf("Failed to connect to node %s: %v", node.ID, err)
        return nil
    }
    return transport.NewTCPStream(conn)
}



func (m *MasterNode) startDeleteTimer(){
    log.Println("=======Start Delete Timer=======")
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <- ticker.C:
            log.Printf("TICK DELETE TIMER : %v",time.Now())

            go func(){
                ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
                defer cancel()
                if err := m.deleteTimer(ctx); err != nil{
                    log.Printf("Error in deleteTimer : %v", err)
                }
            }()
        case <- m.stopChan:
            return
        }
    }
}

func (m *MasterNode) deleteTimer(ctx context.Context) error{
    log.Println("=======Starting delete timer operation=======")

    batchSize := 1000
    stopDel := false
    var cursor string

    for{
        select {
        case <- ctx.Done():
            return ctx.Err()
        default:
        }

        keyData, err := m.kvServer.ScanKey("delete_file:",cursor,batchSize)
        if err != nil{
            log.Printf("Error scanning keys: %v", err)
            return err
        }

        if len(keyData.Keys) ==0{
            log.Println("No more keys to process")
            break
        }

        delBatch := kvclient.NewKVBatch()
        var wg sync.WaitGroup
        errChan := make(chan error, len(keyData.Keys))

        for _, deleteKey := range keyData.Keys{
            parse_data := strings.Split(deleteKey, ":")

            if len(parse_data) != 4{
                delBatch.Add("delete",deleteKey, "")
                continue
            }

            delTime , err := time.Parse("20060102150405",parse_data[1])
            if err != nil{
                errChan <- fmt.Errorf("invalid timestamp in key %s: %v",deleteKey, err)
                delBatch.Add("delete",deleteKey, "")
                continue
            }


            log.Printf("Set up stop del if time : %v before delTime : %v", time.Now(),delTime)
            
            if time.Now().Before(delTime){
                stopDel = true
                break
            }
            wg.Add(1)
            go func(userid,filename,key string){
                defer wg.Done()
                delReq := &transport.DeleteFileRequest{
                    Filename: filename,
                    UserID: userid,
                }
                if err := m.DeleteFile(ctx, delReq); err != nil{
                    log.Printf("Delete File Error : %v",err)
                    errChan <- fmt.Errorf("failed to delete file for key %s: %v", key, err)
                    return
                }

                delBatch.Add("delete",key,"")
                log.Printf("Delete for batch set :%v",key)
            }(parse_data[2], parse_data[3],deleteKey)
        }
        log.Printf("Delete for loop end")
        
        wg.Wait()
        close(errChan)


        for err := range errChan{
            log.Printf("Error during deletion : %v", err)
        }

        if err := m.kvServer.BatchOperation(delBatch.GetPairs()); err != nil {
            log.Printf("Error in batch deletion of keys: %v", err)
            return err
        }

        log.Printf("Stop delete is : %v", stopDel)
        if stopDel{
            break
        }
        cursor = keyData.NextCursor
    }

    log.Printf("Start error log delete")
    var errorWg sync.WaitGroup
    for nodeID,node := range m.dataNodes {
        errorWg.Add(1)
        go func(node *DataNodeInfo) {
            defer errorWg.Done()
            startKey := fmt.Sprintf("error_delete:%s:", node.ID)
            errorCursor := ""



            errorBatch := kvclient.NewKVBatch()
            for {
                errorKeyData, err := m.kvServer.ScanKey(startKey, errorCursor, batchSize)
                if err != nil {
                    log.Printf("Error scanning error_delete keys for node %s: %v", nodeID, err)
                    return
                }
                log.Printf("Error batch node : %v, key : %v", node, startKey)
                if len(errorKeyData.Keys) == 0 {
                    break
                }
                for _, errorKey := range errorKeyData.Keys {
                    errorParts := strings.Split(errorKey, ":")
                    if len(errorParts) == 4 {
                        userID, fileName := errorParts[2], errorParts[3]
                        conn, err := net.Dial("tcp",node.Addr)
                        if err != nil{
                            return
                        }
                        stream := transport.NewTCPStream(conn)
                        
                        var wg sync.WaitGroup
                        wg.Add(1)
                        go func(_stream *transport.StreamService, user_id, file_name string){
                            defer _stream.CloseStream()
                            defer wg.Done()
                            err = m.deleteFileFromDataNode(ctx,_stream,user_id, file_name)
                            if err != nil{
                                log.Printf("Delete Error log file : %v",err)
                            }else{
                                errorBatch.Add("delete", errorKey, "")
                            }
                        }(stream, userID, fileName)
                        wg.Wait()
                    }
                    
                }
                if len(errorBatch.GetPairs()) > 0 {
                    if err := m.kvServer.BatchOperation(errorBatch.GetPairs()); err != nil {
                        log.Printf("Error in batch deletion of error_delete keys: %v", err)
                        break
                    }
                }
                if errorKeyData.NextCursor == "" {
                    break
                }
                startKey = errorKeyData.NextCursor
            }
        }(node)
    }
    errorWg.Wait()

    log.Printf("End delete timer operation")

    return nil
}

func (m *MasterNode) GetFileMetadata(ctx context.Context, bucket, filename string)(*transport.FileMetadata, error){
    key := fmt.Sprintf("file:%s:%s",bucket,filename)

    binData,err := m.kvServer.Get(key)
    if err !=nil{
        return nil, fmt.Errorf("error File Metadata not exist")
    }

    var ret transport.FileMetadata
    if err := json.Unmarshal(binData, &ret); err != nil {
        if len(binData) == 0 {
            return nil, fmt.Errorf("file metadata not found")
        }
        return nil, fmt.Errorf("failed to unmarshal file metadata: %w", err)
    }

    return &ret,nil
}

func (m *MasterNode) HasBucket(ctx context.Context, bucket string)(*transport.BucketMetadata, error){

    key := fmt.Sprintf("bucket:%s",bucket)

    resp,err := m.kvServer.Get(key)

    if err!=nil{
        return nil, err
    }

    var metadata transport.BucketMetadata
    err = json.Unmarshal(resp, &metadata)
    if err != nil {
        return nil, err
    }
    
    return &metadata,nil
}

func (m *MasterNode) uploadChunksToNodes(ctx context.Context, req *transport.UploadFileRequest, chunks []*transport.FileChunk, replica int) ([]string, error) {
    var wg sync.WaitGroup
    errChan := make(chan error, len(chunks)*replica)
    chunkMetas := make(map[string]*transport.ChunkMetadata)
    var chunkMetasMutex sync.Mutex

    // var tmp_node_list map[string]bool
    tmp_node_list := make(map[string]bool)

    for inx, chunk := range chunks {
        chunkKey := fmt.Sprintf("chunk:%s:%s:%05d", req.UserID, req.Filename, inx)
        streamPairs := m.getStream(replica)

        for _, pair := range streamPairs {
            wg.Add(1)
            go func(pair StreamNodePair, chunkIndex int, chunkContent *transport.FileChunk) {
                defer wg.Done()
                err := m.sendChunkToDataNode(ctx, pair.Stream, req.UserID, req.Filename, chunkContent)
                if err != nil {
                    errChan <- fmt.Errorf("failed to upload chunk %d to node %s: %v", chunkIndex, pair.Node.ID, err)
                    return
                }

                chunkMetasMutex.Lock()
                if _, exists := chunkMetas[chunkKey]; !exists {
                    chunkMetas[chunkKey] = &transport.ChunkMetadata{
                        ChunkIndex: chunkIndex,
                        NodeID:     []string{},
                        Size:       int64(len(chunkContent.Content)),
                    }
                }


                tmp_node_list[pair.Node.ID] = true
                chunkMetas[chunkKey].NodeID = append(chunkMetas[chunkKey].NodeID, pair.Node.ID)
                chunkMetasMutex.Unlock()
            }(pair, inx, chunk)
        }
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()

    var uploadErrors []error
    for err := range errChan {
        uploadErrors = append(uploadErrors, err)
    }

    for _, er := range uploadErrors{
        log.Printf("Upload error occur : %v", er)
    }    



    // 하나라도 청크 업로드 실패 시 전체 삭제 하고 리턴
    for inx,chunk := range chunkMetas{
        if len(chunk.NodeID) == 0{
            go m.asyncDeleteUploadedChunks(req.UserID, req.Filename, tmp_node_list)
            return nil, fmt.Errorf("failed to save chunk : %v", inx)
        }
    }



    // 청크 메타데이터 저장
    chunkMetaBatch := kvclient.NewKVBatch()
    for key, chunkMeta := range chunkMetas {
        chunkMetaBatch.Add("set", key, chunkMeta)
    }
    if err := m.kvServer.BatchOperation(chunkMetaBatch.GetPairs()); err != nil {
        return nil,fmt.Errorf("failed to save chunk metadata: %v", err)
    }


    var ret_node_list []string
    for id := range tmp_node_list{
        ret_node_list = append(ret_node_list, id)
    }

    return ret_node_list,nil
}



func (m *MasterNode) asyncDeleteUploadedChunks(userID, filename string, nodeList map[string]bool) {
    // 입력받은 ctx를 기반으로 새로운 컨텍스트를 생성합니다.
    // 5분의 타임아웃을 설정하지만, 부모 컨텍스트가 취소되면 함께 취소됩니다.
    deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
    defer cancel()

    var wg sync.WaitGroup
    for nodeID := range nodeList {
        wg.Add(1)
        go func(nodeID string) {
            defer wg.Done()
            select {
            case <-deleteCtx.Done():
                log.Printf("Deletion cancelled for node %s: %v", nodeID, deleteCtx.Err())
                return
            default:
                conn, err := net.DialTimeout("tcp", m.dataNodes[nodeID].Addr, 10*time.Second)
                if err != nil {
                    log.Printf("Failed to connect to node %s for deletion: %v", nodeID, err)
                    return
                }
                defer conn.Close()
                
                stream := transport.NewTCPStream(conn)
                if err := m.deleteFileFromDataNode(deleteCtx, stream, userID, filename); err != nil {
                    log.Printf("Failed to delete file from node %s: %v", nodeID, err)
                } else {
                    log.Printf("Successfully deleted file from node %s", nodeID)
                }
            }
        }(nodeID)
    }

    // 비동기적으로 완료를 기다립니다.
    go func() {
        wg.Wait()
        log.Printf("Async deletion process completed for user %s, file %s", userID, filename)
    }()

    log.Printf("Async deletion process started for user %s, file %s", userID, filename)
}


func (m *MasterNode) updateBucketMetadata(ctx context.Context, userID string, fileSize int64) error {
    buckKey := fmt.Sprintf("bucket:%s", userID)
    buckMetadata, _ := m.HasBucket(ctx, userID)
    if buckMetadata == nil {
        buckMetadata = &transport.BucketMetadata{
            BucketSize: fileSize,
            FileCnt:    1,
        }
    } else {
        buckMetadata.BucketSize += fileSize
        buckMetadata.FileCnt++
    }
    return m.kvServer.Set(buckKey, buckMetadata)
}

func (m *MasterNode) GetConnectedDataNodesCount() int {
    return len(m.dataNodes)
}



