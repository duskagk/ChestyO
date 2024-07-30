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
	"sort"
	"strings"
	"sync"
	"time"
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
	// resultChan := make(chan (bool))
	// go m.hasFileAsync(ctx,userId, filename, resultChan)
	return false
}

// node/master.go
func (m *MasterNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {

	fileExists := m.HasFile(ctx,req.UserID, req.Filename)

	if fileExists {
		switch req.Policy {
		case policy.Overwrite:
			return m.handleOverwrite(ctx, req, createStream)
		default:
			return fmt.Errorf("can't")
		}
	} else {
		// 새 파일 업로드
		return m.handleNewUpload(ctx, req, createStream)
	}
}

func (m *MasterNode) handleNewUpload(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {

    log.Printf("MasterNode: Handling new upload for file %s from user %s", req.Filename, req.UserID)
    // 3. Split the file into chunks
    chunks := utils.SplitFileIntoChunks(req.Content)

    // 4. Select DataNodes for upload
    selectedNodes := m.selectDataNodesForUpload(len(chunks))
    if len(selectedNodes) == 0 {
        return fmt.Errorf("no available DataNodes for upload")
    }

    
    // 5. Distribute chunks to DataNodes
    chunkDistribution := m.distributeChunks(chunks, selectedNodes)

    // 6. Upload chunks to DataNodes
    err := m.uploadChunksToDataNodes(ctx, req, chunkDistribution)
    if err != nil {
        return fmt.Errorf("failed to upload chunks: %v", err)
    }

    log.Printf("MasterNode: Successfully uploaded file %s for user %s", req.Filename, req.UserID)
    return nil
}


func (m *MasterNode) sendChunkToDataNode(ctx context.Context, dataNode *DataNodeInfo, req *transport.UploadFileRequest, chunk *transport.FileChunk) error {
    log.Printf("MasterNode: Starting to send chunk %d to DataNode %s\n", chunk.Index, dataNode.ID)

    // Chunk 데이터 전송
    chunkMsg := &transport.Message{
        Type:        transport.MessageType_UPLOAD_CHUNK,
        UploadChunk: &transport.UploadFileChunk{
            UserID    :req.UserID,
            Filename  :req.Filename,
            Chunk     : *chunk,
        },
    }
    err := transport.SendMessage(dataNode.Conn, chunkMsg)
    if err != nil {
        log.Printf("MasterNode: Error sending chunk data to DataNode %s: %v\n", dataNode.ID, err)
        return fmt.Errorf("failed to send chunk: %v", err)
    }
    log.Printf("MasterNode: Successfully sent chunk data to DataNode %s\n", dataNode.ID)

    // 응답 대기
    respChan := make(chan *transport.Message, 1)
    errChan := make(chan error, 1)

    go func() {
        respMsg, err := transport.ReceiveMessage(dataNode.Conn)
        if err != nil {
            errChan <- fmt.Errorf("failed to receive response: %v", err)
            return
        }
        respChan <- respMsg
    }()

    select {
    case <-ctx.Done():
        return ctx.Err()
    case respMsg := <-respChan:
        log.Printf("MasterNode: Received response from DataNode %s: %+v\n", dataNode.ID, respMsg)
        
        // respMsg 처리
        if respMsg.Type != transport.MessageType_UPLOAD_CHUNK_RESPONSE {
            return fmt.Errorf("unexpected response type: %v", respMsg.Type)
        }
        
        if respMsg.UploadChunkResponse == nil {
            return fmt.Errorf("null UploadChunkResponse")
        }
        
        if !respMsg.UploadChunkResponse.Success {
            return fmt.Errorf("chunk upload failed: %s", respMsg.UploadChunkResponse.Message)
        }
        
        if respMsg.UploadChunkResponse.ChunkIndex != chunk.Index {
            return fmt.Errorf("mismatched chunk index: expected %d, got %d", chunk.Index, respMsg.UploadChunkResponse.ChunkIndex)
        }
        
        log.Printf("MasterNode: Chunk %d successfully uploaded to DataNode %s\n", chunk.Index, dataNode.ID)
        return nil

    case err := <-errChan:
        log.Printf("MasterNode: Error receiving response from DataNode %s: %v\n", dataNode.ID, err)
        return err

    case <-time.After(10 * time.Second):  // 타임아웃 시간을 30초로 증가
        log.Printf("MasterNode: Timeout waiting for response from DataNode %s\n", dataNode.ID)
        return fmt.Errorf("timeout waiting for response")
    }
}



func (m *MasterNode) handleOverwrite(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {
	deleteReq := &transport.DeleteFileRequest{
		Filename: req.Filename,
		UserID:   req.UserID,
	}
	_, err := m.handleDeleteFile(ctx, deleteReq)
	if err != nil {
		return fmt.Errorf("failed to delete exist file : %v", err)
	}

	return m.handleNewUpload(ctx, req, createStream)
}


func (m *MasterNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest, stream transport.DownloadStream) error {

	if isExist := m.HasFile(ctx,req.UserID, req.Filename); isExist {
		return m.handleDownloadFile(ctx, req, stream)
	}
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

	return m.handleDeleteFile(ctx, req)
}

func (m *MasterNode) handleDeleteFile(ctx context.Context, req *transport.DeleteFileRequest) (*transport.DeleteFileResponse, error) {
    mu := m.getUserMutex(req.UserID)
    mu.Lock()
    defer mu.Unlock()
    
    log.Printf("Attempting to delete file: %s for user: %s", req.Filename, req.UserID)

    dataNodes := make([]*DataNode, 0, len(m.dataNodes))
    // for _, node := range m.dataNodes {
    //     dataNodes = append(dataNodes, node)
    // }

    var wg sync.WaitGroup
    errChan := make(chan error, len(dataNodes))

    for _, dataNode := range dataNodes {
        wg.Add(1)
        go func(node *DataNode) {
            defer wg.Done()
            response, err := node.DeleteFile(ctx, req)
            if err != nil {
                errChan <- fmt.Errorf("error deleting file %s from node %s: %v", req.Filename, node.ID, err)
            } else if !response.Success {
                errChan <- fmt.Errorf("failed to delete file %s from node %s: %s", req.Filename, node.ID, response.Message)
            }
        }(dataNode)
    }

    wg.Wait()
    close(errChan)

    var errors []string
    for err := range errChan {
        errors = append(errors, err.Error())
    }

    delete(m.fileLocations, req.Filename)
    log.Printf("Removed file %s from fileLocations", req.Filename)

    if len(errors) > 0 {
        return &transport.DeleteFileResponse{
            Success: false,
            Message: fmt.Sprintf("Partial deletion occurred: %s", strings.Join(errors, "; ")),
        }, fmt.Errorf("deletion errors: %s", strings.Join(errors, "; "))
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
    transport, err := transport.NewTCPTransport(addr, m)
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


func (m *MasterNode) hasFileAsync(ctx context.Context,userId, filename string, resultChan chan<- (bool)) {
    mu := m.getUserMutex(userId)
    mu.Lock()
    defer mu.Unlock()

	var wg sync.WaitGroup
	foundFile := false
	done := make(chan struct{})

	for _, dataNode := range m.dataNodes {
		wg.Add(1)
		go func(node *DataNodeInfo) {
			defer wg.Done()
			close(done)
		}(dataNode)
	}

	go func() {
		wg.Wait()
		resultChan <- foundFile
	}()
}

func (m *MasterNode) handleDownloadFile(ctx context.Context, req *transport.DownloadFileRequest, stream transport.DownloadStream) error {
	
    mu := m.getUserMutex(req.UserID)
    mu.Lock()
    defer mu.Unlock()

	log.Printf("Attempting to download file: %s for user: %s", req.Filename, req.UserID)

	fds, ok := stream.(*transport.FileDownloadStream)
	if !ok {
		return fmt.Errorf("unsupported stream type")
	}

	allChunks := make(map[int][]byte)
	var wg sync.WaitGroup



	wg.Wait()

	if len(allChunks) == 0 {
		return fmt.Errorf("file not found: %s", req.Filename)
	}

	// 인덱스를 기준으로 정렬
	var sortedIndexes []int
	for index := range allChunks {
		sortedIndexes = append(sortedIndexes, index)
	}
	sort.Ints(sortedIndexes)

	// 정렬된 청크들을 스트림에 추가
	for _, index := range sortedIndexes {
		fds.AddChunk(allChunks[index])
	}

	log.Printf("Successfully downloaded file %s with %d chunks", req.Filename, len(allChunks))
	return nil

}


func (m *MasterNode) getUserMutex(userID string) *sync.RWMutex {
    mu, _ := m.userMutexes.LoadOrStore(userID, &sync.RWMutex{})
    return mu.(*sync.RWMutex)
}

func (m *MasterNode) handleRegister(msg *transport.RegisterMessage) error {
    fmt.Printf("MasterNode: Received registration request from %v\n", msg)

    m.mu.Lock()
    defer m.mu.Unlock()

    // 이미 등록된 노드인지 확인
    if existingNode, exists := m.dataNodes[msg.NodeID]; exists {
        log.Printf("DataNode %s already registered. Updating address from %s to %s", msg.NodeID, existingNode.Addr, msg.Addr)
        existingNode.Addr = msg.Addr
        // 기존 연결이 있다면 닫기
        if existingNode.Conn != nil {
            existingNode.Conn.Close()
            existingNode.Conn = nil
        }
    } 

    conn, err := net.Dial("tcp", msg.Addr)

    if err !=nil{
        log.Printf("MasterNode : Connect faile with data node")
    }

        // 새 DataNode 정보 저장
    m.dataNodes[msg.NodeID] = &DataNodeInfo{
        ID:   msg.NodeID,
        Addr: msg.Addr,
        Conn: conn,
    }
    

    log.Printf("Registered DataNode: %s at %s", msg.NodeID, msg.Addr)



    return nil
}

func (m *MasterNode) Register(ctx context.Context, req *transport.RegisterMessage) error {
    return m.handleRegister(req)
}

func (m *MasterNode) selectDataNodesForUpload(numChunks int) []*DataNodeInfo {
    m.mu.RLock()
    defer m.mu.RUnlock()

    availableNodes := make([]*DataNodeInfo, 0, len(m.dataNodes))
    for _, node := range m.dataNodes {
        availableNodes = append(availableNodes, node)
    }

    // Implement your node selection strategy here
    // For simplicity, we'll just return all available nodes
    return availableNodes
}

func (m *MasterNode) distributeChunks(chunks []transport.FileChunk, nodes []*DataNodeInfo) map[*DataNodeInfo][]transport.FileChunk {
    distribution := make(map[*DataNodeInfo][]transport.FileChunk)
    for i, chunk := range chunks {
        nodeIndex := i % len(nodes)
        node := nodes[nodeIndex]
        distribution[node] = append(distribution[node], chunk)
    }
    return distribution
}

func (m *MasterNode) uploadChunksToDataNodes(ctx context.Context, req *transport.UploadFileRequest, chunkDistribution map[*DataNodeInfo][]transport.FileChunk) error {
    log.Printf("MasterNode: Starting to upload chunks for file %s", req.Filename)

    var wg sync.WaitGroup
    errChan := make(chan error, len(chunkDistribution))

    for dataNode, chunks := range chunkDistribution {
        wg.Add(1)
        go func(node *DataNodeInfo, nodeChunks []transport.FileChunk) {
            defer wg.Done()
            for _, chunk := range nodeChunks {
                select {
                case <-ctx.Done():
                    errChan <- ctx.Err()
                    return
                default:
                    err := m.sendChunkToDataNode(ctx, node, req, &chunk)
                    if err != nil {
                        errChan <- fmt.Errorf("failed to send chunk %d to DataNode %s: %v", chunk.Index, node.ID, err)
                        return
                    }
                    log.Printf("MasterNode: Successfully sent and processed chunk %d for DataNode %s", chunk.Index, node.ID)
                }
            }
        }(dataNode, chunks)
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()

    for err := range errChan {
        if err != nil {
            log.Printf("MasterNode: Error during chunk upload: %v", err)
            return err
        }
    }

    log.Printf("MasterNode: All chunks uploaded successfully for file %s", req.Filename)
    return nil
}

func (m *MasterNode) UploadFileChunk(ctx context.Context, chunk *transport.UploadFileChunk) error{
    return nil
}

func (m *MasterNode) GetConnectedDataNodesCount() int {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return len(m.dataNodes)
}