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
    restServer      *rest.RestServer
    stopChan        chan struct{}
    kvServer        *kvclient.KVClient
    nodeMu          sync.Mutex
    nodeKeys        []string                 // 맵의 키를 저장할 리스트
	currentIndex    int                      // 현재 라운드 로빈 인덱스
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

    selNodes := m.selectDataNode(1)

    var wg sync.WaitGroup
    errChan := make(chan error, 1)

    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    chunkMetas := make(map[string]*transport.ChunkMetadata)
    var chunkMetasMutex sync.Mutex


    chunkMetaBatch := kvclient.NewKVBatch()


    for _, node := range selNodes {
        conn, err := net.Dial("tcp",node.Addr)
        if err!=nil{
            errChan <- err
        }
        stream := transport.NewTCPStream(conn)
        for i, chunk := range chunks{
            wg.Add(1)
            
            go func(inx int,chunk *transport.FileChunk,nodeID string){
                defer wg.Done()
                if err := m.sendChunkToDataNode(ctx,stream,req.UserID,req.Filename,chunk); err!=nil{
                    errChan <- err
                }
                
                chunkKey := fmt.Sprintf("chunk:%s:%s:%05d", req.UserID, req.Filename, inx)
                
                chunkMetasMutex.Lock()
                if _, exists := chunkMetas[chunkKey]; !exists {
                    chunkMetas[chunkKey] = &transport.ChunkMetadata{
                        ChunkIndex: inx,
                        NodeID:     []string{},
                        Size:       int64(len(chunk.Content)),
                    }
                }
                chunkMetas[chunkKey].NodeID = append(chunkMetas[chunkKey].NodeID, nodeID)
                chunkMetasMutex.Unlock()
            }(i,chunk,node.ID)
        }
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

    for key,chunk_meta := range chunkMetas{
        metaJSON, _ := json.Marshal(chunk_meta)
        chunkMetaBatch.Add("set",key,metaJSON)
    }

    log.Printf("MasterNode: Upload completed for file %s", req.Filename)

    nodeIDs := make([]string, len(selNodes))
    for i, node := range selNodes {
        nodeIDs[i] = node.ID
    }


    metadata := transport.FileMetadata{
        FileSize:      int64(len(req.Content)),
        ChunkNodes:    nodeIDs,
        ContentType: req.FileContentType,
        TotalChunks: int64(len(chunks)),
        ChunkSize: utils.ChunkSize,
    }

    if req.RetentionPeriodDays>0{
        metadata.RetentionTime = time.Now().AddDate(0,0,req.RetentionPeriodDays)
    }else if req.RetentionPeriodDays==0{
        metadata.RetentionTime = time.Now().AddDate(0,0,90)
    }

    key := fmt.Sprintf("file:%s:%s", req.UserID, req.Filename)
    metadataJSON, err := json.Marshal(metadata)
    if err !=nil{
        return err
    }
    // 현재 시간
    m.kvServer.Set(key, string(metadataJSON))
    
    if !metadata.RetentionTime.IsZero(){
        delete_key := fmt.Sprintf("delete_file:%s:%s:%s",
        metadata.RetentionTime.Format("20060102150405"),
        req.UserID,
        req.Filename)
        m.kvServer.Set(delete_key, "")
    }

    buck_metadata, _ := m.HasBucket(ctx,req.UserID)
    if buck_metadata ==nil{
        buck_metadata = &transport.BucketMetadata{}
        buck_metadata.BucketSize= req.FileSize
        buck_metadata.FileCnt= 1
    }else{
        buck_metadata.BucketSize += req.FileSize
        buck_metadata.FileCnt++
    }
    buck_key := fmt.Sprintf("bucket:%s",req.UserID)
    buckdataJson ,_ := json.Marshal(metadata)
    m.kvServer.Set(buck_key,string(buckdataJson))

    if err := m.kvServer.BatchOperation(chunkMetaBatch.GetPairs()); err != nil {
        log.Printf("MasterNode: Error setting chunk metadata: %v", err)
        return err
    }

    return nil
}


func (m *MasterNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest) ([]*transport.FileChunk, error) {
    log.Printf("MasterNode: Starting download for file %s from user %s", req.Filename, req.UserID)

    hasFile, metadata, err := m.HasFile(ctx, req.UserID, req.Filename)
    if err != nil {
        return nil, fmt.Errorf("error checking file existence: %v", err)
    }
    if !hasFile {
        return nil, fmt.Errorf("file not found: %s", req.Filename)
    }

    var startChunk int64
    var endChunk int64

    if req.End ==-1{
        startChunk = 0
        endChunk = metadata.TotalChunks
    }else{
        startChunk = req.Start / metadata.ChunkSize
        endChunk = req.End / metadata.ChunkSize
    
        if endChunk-startChunk > 10 {
            endChunk = startChunk + 10
        }
    }


    prefix := fmt.Sprintf("chunk:%s:%s", req.UserID, req.Filename)
    cursor := fmt.Sprintf("%s:%05d", prefix, startChunk)

    chunkMetadata, _, err := m.kvServer.ScanValueByKey(prefix, cursor, int(endChunk-startChunk))
    if err != nil {
        return nil, fmt.Errorf("error fetching chunk metadata: %v", err)
    }

    chunksChan := make(chan *transport.FileChunk, endChunk-startChunk)
    errChan := make(chan error, endChunk-startChunk)
    var wg sync.WaitGroup

    for _, chunkData := range chunkMetadata {
        wg.Add(1)
        var chunkMeta transport.ChunkMetadata
        
        valueJSON, err := json.Marshal(chunkData["value"])
        if err != nil {
            log.Printf("Error marshaling chunk metadata: %v", err)
            wg.Done()
            continue
        }
        
        // JSON 문자열을 ChunkMetadata 구조체로 언마샬
        if err := json.Unmarshal(valueJSON, &chunkMeta); err != nil {
            log.Printf("Error unmarshaling chunk metadata: %v", err)
            wg.Done()
            continue
        }

        go func(chunk_meta transport.ChunkMetadata) {
            defer wg.Done()

            res_chunk, err := m.requestChunkStream(ctx, chunk_meta.NodeID[0], chunk_meta.ChunkIndex, req)
            if err != nil {
                log.Printf("Error requesting chunk %d: %v", chunk_meta.ChunkIndex, err)
                errChan <- err
            }

            if chunk_meta.ChunkIndex == int(startChunk) {
                res_chunk.Content = res_chunk.Content[req.Start%metadata.ChunkSize:]
            }

            chunksChan <- res_chunk
            
            log.Printf("Push Chunk Channel Index %v Len %v", res_chunk.Index, len(res_chunk.Content))

        }(chunkMeta)
    }

    go func() {
        wg.Wait()
        close(chunksChan)
    }()

    // Collect all chunks from the channel
    var chunks []*transport.FileChunk
    for chunk := range chunksChan {
        chunks = append(chunks, chunk)
        req.Length += int64(len(chunk.Content))
    }

    // Sort chunks by ChunkIndex
    sort.Slice(chunks, func(i, j int) bool {
        return chunks[i].Index < chunks[j].Index
    })

    return chunks, nil
}

func (m *MasterNode) requestChunkStream(ctx context.Context,node_id string,chunk_index int,req *transport.DownloadFileRequest) (*transport.FileChunk,error){
    conn, err := net.Dial("tcp", m.dataNodes[node_id].Addr)
    
    if err!=nil{
        return nil,fmt.Errorf("failed to connect to DataNode %s: %v", node_id, err)
    }
    defer conn.Close()

    stream := transport.NewTCPStream(conn)
    request := &transport.Message{
        Category:  transport.MessageCategory_REQUEST,
        Operation: transport.MessageOperation_DOWNLOAD_CHUNK,
        Payload: &transport.RequestPayload{
            DownLoadChunk: &transport.DownloadChunkRequest{
                UserID:   req.UserID,
                Filename: req.Filename,
                ChunkIndex: chunk_index,
            },
        },
    }

    if err := stream.Send(request); err != nil {
        return nil,fmt.Errorf("failed to send download request to DataNode %s: %v", node_id, err)
    }

    response, err := stream.Recv()
    if err == io.EOF {
        return nil,fmt.Errorf("The stream from DataNode %s is end", node_id)
    }
    if err != nil {
        return nil,fmt.Errorf("error receiving chunk from DataNode %s: %v", node_id, err)
    }

    if response.Category == transport.MessageCategory_RESPONSE && response.Operation == transport.MessageOperation_DOWNLOAD_CHUNK {
        payload, ok := response.Payload.(*transport.ResponsePayload)
        if !ok || payload.DownloadChunk == nil {
            return nil,fmt.Errorf("invalid payload for download chunk from DataNode %s", node_id)
        }
        if payload.DownloadChunk.Success && payload.DownloadChunk.Message == "All chunks sent" {
            log.Printf("All chunks received from DataNode %s", node_id)
        }
        return &payload.DownloadChunk.Chunk,nil
    }
    return nil,fmt.Errorf("No Data request")
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
    errChan := make(chan error, len(metaData.ChunkNodes))

    var node_err_list []string

    for _,nodeID := range metaData.ChunkNodes {
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
                node_err_list = append(node_err_list, nodeID)
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
        if err !=nil{
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

    for _,err_node :=range node_err_list {
        key := fmt.Sprintf("error:%s:%s:%s",err_node,req.UserID,req.Filename)
        batch.Add("set",key,"")
    }

    err = m.kvServer.BatchOperation(batch.GetPairs());

    if err!=nil{
        return fmt.Errorf("error: Batch operation error : %v", err)
    }

    log.Printf("MasterNode: Successfully deleted file %s for user %s", req.Filename, req.UserID)
    return nil
}


func (m *MasterNode) deleteFileFromDataNode(ctx context.Context, addr, userID, filename string) error {
    conn, err := net.Dial("tcp", addr)
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
    opCtx := context.Context(context.Background())

    start_key := fmt.Sprintf("error:%s:",req.NodeID)
    cursor := ""
    batch := kvclient.NewKVBatch()
    for{
        Scanres,err := m.kvServer.ScanKey(start_key,cursor,1000)
        if err!=nil{
            break
        }
        for _,key := range Scanres.Keys{
            datas := strings.Split(key, ":")
            if len(datas)==4{
                go func(user_id, file_name string){
                    err:= m.deleteFileFromDataNode(opCtx, req.Addr, user_id, file_name)
                    if err==nil{
                        batch.Add("delete",key,"")
                    }
                }(datas[2],datas[3])
            }
        }
        if Scanres.NextCursor ==""{
            break
        }
        cursor = Scanres.NextCursor
    }

    if err:=m.kvServer.BatchOperation(batch.GetPairs()); err!=nil{
        log.Printf("Error when error operation %v", err)
        return err
    }

    m.dataNodes[req.NodeID] = nodeInfo
    m.nodeKeys = append(m.nodeKeys, req.NodeID)

    log.Printf("Registered DataNode: %s at %s", req.NodeID, req.Addr)
    return nil
}


func (m *MasterNode) deleteDataNode(nodeID string){
    m.nodeMu.Lock()
    defer m.nodeMu.Unlock()
    if _, exists := m.dataNodes[nodeID]; !exists{
        return
    }
    for i, key := range m.nodeKeys{
        if key == nodeID{
            m.nodeKeys = append(m.nodeKeys[:i], m.nodeKeys[i+1:]...)
            break
        }
    }
    if m.currentIndex >= len(m.nodeKeys){
        m.currentIndex = 0
    }

}


func (m *MasterNode) Register(ctx context.Context, req *transport.RegisterMessage) error {
    log.Printf("Register method")
    return m.handleRegister(req)
}



func (m *MasterNode) selectDataNode(replica_num int) []*DataNodeInfo {
    m.nodeMu.Lock()
    defer m.nodeMu.Unlock()

    selectedNodes := []*DataNodeInfo{}

    if len(m.nodeKeys) == 0 {
        return nil // 노드가 없는 경우 빈 슬라이스 반환
    }

    // replica_num이 노드 수보다 크다면 모든 노드를 선택
    if replica_num >= len(m.nodeKeys) {
        for _, key := range m.nodeKeys {
            selectedNodes = append(selectedNodes, m.dataNodes[key])
        }
        return selectedNodes
    }

    // replica_num만큼의 노드를 선택
    for i := 0; i < replica_num; i++ {
        selKey := m.nodeKeys[m.currentIndex]
        selectedNodes = append(selectedNodes, m.dataNodes[selKey])
        m.currentIndex = (m.currentIndex + 1) % len(m.nodeKeys)
    }

    return selectedNodes
}


func (m *MasterNode) startDeleteTimer(){
    ticker := time.NewTicker(1 * time.Hour)
    defer ticker.Stop()

    for {
        select {
        case <- ticker.C:
            ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
            if err := m.deleteTimer(ctx) ; err != nil{
                log.Printf("Error in deleteTimer: %v",err)
            }
            cancel()
        case <- m.stopChan:
            return
        }
    }
}

func (m *MasterNode) deleteTimer(ctx context.Context) error{
    log.Println("Starting delete timer operation")

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
            wg.Add(1)

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

            if time.Now().Before(delTime){
                stopDel = true
                break
            }

            go func(userid,filename,key string){
                defer wg.Done()
                delReq := &transport.DeleteFileRequest{
                    Filename: filename,
                    UserID: userid,
                }
                if err := m.DeleteFile(ctx, delReq); err != nil{
                    errChan <- fmt.Errorf("failed to delete file for key %s: %v", key, err)
                    return
                }

                delBatch.Add("delete",key,"")

            }(parse_data[2], parse_data[3],deleteKey)
        }

        wg.Wait()
        close(errChan)


        for err := range errChan{
            log.Printf("Error during deletion : %v", err)
        }

        if err := m.kvServer.BatchOperation(delBatch.GetPairs()); err != nil {
            log.Printf("Error in batch deletion of keys: %v", err)
            return err
        }

        if stopDel{
            break
        }
        cursor = keyData.NextCursor
    }

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


func (m *MasterNode) GetConnectedDataNodesCount() int {
    return len(m.dataNodes)
}



