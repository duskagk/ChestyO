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
	"strings"
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
	// resultChan := make(chan (bool))
	// go m.hasFileAsync(ctx,userId, filename, resultChan)
	return false
}

// node/master.go
func (m *MasterNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest) error {

	fileExists := m.HasFile(ctx,req.UserID, req.Filename)

	if fileExists {
		switch req.Policy {
		case policy.Overwrite:
			return m.handleOverwrite(ctx, req)
		default:
			return fmt.Errorf("can't")
		}
	} else {
		// 새 파일 업로드
		return m.handleNewUpload(ctx, req)
	}
}

func (m *MasterNode) handleNewUpload(ctx context.Context, req *transport.UploadFileRequest) error {
    log.Printf("MasterNode: Handling new upload for file %s from user %s", req.Filename, req.UserID)

    chunks := utils.SplitFileIntoChunks(req.Content)
    availableNodes := m.getAvailableDataNodes()
    if len(availableNodes) == 0 {
        return fmt.Errorf("no available data nodes for upload")
    }

    chunkDistribution := make(map[string][]transport.FileChunk)
    for i, chunk := range chunks {
        nodeIndex := i % len(availableNodes)
        nodeID := availableNodes[nodeIndex].ID
        chunkDistribution[nodeID] = append(chunkDistribution[nodeID], chunk)
    }

    var failedChunks []transport.FileChunk
    for nodeID, nodeChunks := range chunkDistribution {
        err := m.sendChunksToDataNode(ctx, nodeID, req.UserID, req.Filename, nodeChunks)
        if err != nil {
            log.Printf("Failed to send chunks to DataNode %s: %v. Will retry with other nodes.", nodeID, err)
            failedChunks = append(failedChunks, nodeChunks...)
            delete(chunkDistribution, nodeID)
            // 실패한 노드를 사용 가능한 노드 목록에서 제거
            availableNodes = removeNode(availableNodes, nodeID)
        }
    }

    // 실패한 청크들을 남은 노드들에게 재분배
    if len(failedChunks) > 0 {
        if len(availableNodes) == 0 {
            return fmt.Errorf("no available nodes left to handle failed chunks")
        }
        for i, chunk := range failedChunks {
            nodeIndex := i % len(availableNodes)
            nodeID := availableNodes[nodeIndex].ID
            err := m.sendChunksToDataNode(ctx, nodeID, req.UserID, req.Filename, []transport.FileChunk{chunk})
            if err != nil {
                return fmt.Errorf("failed to redistribute chunk %d: %v", chunk.Index, err)
            }
            chunkDistribution[nodeID] = append(chunkDistribution[nodeID], chunk)
        }
    }

    log.Printf("MasterNode: Successfully uploaded file %s for user %s", req.Filename, req.UserID)
    return nil
}


func (m *MasterNode) sendChunkToDataNode(ctx context.Context, dataNode *DataNodeInfo, req *transport.UploadFileRequest, chunk *transport.FileChunk) error {
    log.Printf("MasterNode: Starting to send chunk %d to DataNode %s", chunk.Index, dataNode.ID)

    if dataNode.Conn == nil {
        return fmt.Errorf("connection to DataNode %s is nil", dataNode.ID)
    }



    chunkMsg := &transport.Message{
        Category   :transport.MessageCategory_REQUEST,
        Operation:  transport.MessageOperation_UPLOAD_CHUNK,
        Payload: &transport.UploadFileChunk{
            UserID: req.UserID,
            Filename: req.Filename,
            Chunk: *chunk,
        },
    }

    err := transport.SendMessage(dataNode.Conn, chunkMsg)
    if err != nil {
        log.Printf("MasterNode: Error sending chunk data to DataNode %s: %v", dataNode.ID, err)
        return fmt.Errorf("failed to send chunk: %v", err)
    }

    log.Printf("MasterNode: Successfully sent chunk data to DataNode %s", dataNode.ID)
    return nil
}


func (m *MasterNode) handleOverwrite(ctx context.Context, req *transport.UploadFileRequest) error {
	deleteReq := &transport.DeleteFileRequest{
		Filename: req.Filename,
		UserID:   req.UserID,
	}
	_, err := m.handleDeleteFile(ctx, deleteReq)
	if err != nil {
		return fmt.Errorf("failed to delete exist file : %v", err)
	}

	return m.handleNewUpload(ctx, req)
}


func (m *MasterNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest) error {

	if isExist := m.HasFile(ctx,req.UserID, req.Filename); isExist {
		return m.handleDownloadFile(ctx, req)
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

func (m *MasterNode) handleDownloadFile(ctx context.Context, req *transport.DownloadFileRequest) error {
	
    // mu := m.getUserMutex(req.UserID)
    // mu.Lock()
    // defer mu.Unlock()

	// log.Printf("Attempting to download file: %s for user: %s", req.Filename, req.UserID)

	// fds, ok := stream.(*transport.FileDownloadStream)
	// if !ok {
	// 	return fmt.Errorf("unsupported stream type")
	// }

	// allChunks := make(map[int][]byte)
	// var wg sync.WaitGroup



	// wg.Wait()

	// if len(allChunks) == 0 {
	// 	return fmt.Errorf("file not found: %s", req.Filename)
	// }

	// // 인덱스를 기준으로 정렬
	// var sortedIndexes []int
	// for index := range allChunks {
	// 	sortedIndexes = append(sortedIndexes, index)
	// }
	// sort.Ints(sortedIndexes)

	// // 정렬된 청크들을 스트림에 추가
	// for _, index := range sortedIndexes {
	// 	fds.AddChunk(allChunks[index])
	// }

	// log.Printf("Successfully downloaded file %s with %d chunks", req.Filename, len(allChunks))
	return nil

}


func (m *MasterNode) getUserMutex(userID string) *sync.RWMutex {
    mu, _ := m.userMutexes.LoadOrStore(userID, &sync.RWMutex{})
    return mu.(*sync.RWMutex)
}

func (m *MasterNode) handleRegister(msg *transport.RegisterMessage) error {
    log.Printf("MasterNode: Received registration request from %v\n", msg)

    m.mu.Lock()
    defer m.mu.Unlock()

    // 이미 등록된 노드인지 확인
    if existingNode, exists := m.dataNodes[msg.NodeID]; exists {
        log.Printf("DataNode %s already registered. Updating address from %s to %s", msg.NodeID, existingNode.Addr, msg.Addr)
        existingNode.Addr = msg.Addr
        if existingNode.Conn != nil {
            existingNode.Conn.Close()
            existingNode.Conn = nil
        }
    } 
    conn, err := net.Dial("tcp", msg.Addr)
    if err !=nil{
        log.Printf("MasterNode : Connect faile with data node")
    }
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
    
    // 총 청크 수를 계산합니다
    totalChunks := 0
    for _, chunks := range chunkDistribution {
        totalChunks += len(chunks)
    }
    responseChan := make(chan *transport.UploadChunkResponse, totalChunks)

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
                    log.Printf("Master : send chunk to datanode")
                    if err != nil {
                        errChan <- fmt.Errorf("failed to send chunk %d to DataNode %s: %v", chunk.Index, node.ID, err)
                        return
                    }
                    
                    // DataNode로부터의 응답 대기
                    // response, err := m.waitForDataNodeResponse(node)
                    if err != nil {
                        errChan <- fmt.Errorf("failed to receive response for chunk %d from DataNode %s: %v", chunk.Index, node.ID, err)
                        return
                    }
                    
                    // responseChan <- response
                    log.Printf("MasterNode: Successfully sent and processed chunk %d for DataNode %s", chunk.Index, node.ID)
                }
            }
        }(dataNode, chunks)
    }

    // 모든 고루틴이 완료될 때까지 대기
    go func() {
        wg.Wait()
        close(errChan)
        close(responseChan)
    }()

    // 에러 처리
    for err := range errChan {
        if err != nil {
            log.Printf("MasterNode: Error during chunk upload: %v", err)
            return err
        }
    }

    // 응답 처리
    successfulUploads := 0
    for response := range responseChan {
        if response.Success {
            successfulUploads++
        } else {
            log.Printf("MasterNode: Chunk upload failed: %s", response.Message)
        }
    }

    if successfulUploads != totalChunks {
        return fmt.Errorf("not all chunks were uploaded successfully: %d/%d", successfulUploads, totalChunks)
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


// 사용 가능한 DataNode 목록을 반환하는 헬퍼 함수
func (m *MasterNode) getAvailableDataNodes() []*DataNodeInfo {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    nodes := make([]*DataNodeInfo, 0, len(m.dataNodes))
    for _, node := range m.dataNodes {
        nodes = append(nodes, node)
    }
    return nodes
}

func (m *MasterNode) sendChunksToDataNode(ctx context.Context, nodeID, userID, filename string, chunks []transport.FileChunk) error {
    node, ok := m.dataNodes[nodeID]
    if !ok {
        return fmt.Errorf("DataNode %s not found", nodeID)
    }

    conn, err := net.Dial("tcp", node.Addr)
    if err != nil {
        return fmt.Errorf("failed to connect to DataNode %s: %v", nodeID, err)
    }
    defer conn.Close()

    encoder := gob.NewEncoder(conn)
    decoder := gob.NewDecoder(conn)

    // 청크 전송을 위한 고루틴 생성
    chunkChan := make(chan transport.FileChunk)
    errChan := make(chan error)
    doneChan := make(chan bool)

    go func() {
        for chunk := range chunkChan {
            retries := 3
            for retries > 0 {
                req := &transport.Message{
                    Category: transport.MessageCategory_REQUEST,
                    Operation: transport.MessageOperation_UPLOAD_CHUNK,
                    Payload: &transport.RequestPayload{
                        UploadChunk: &transport.UploadFileChunk{
                            UserID:   userID,
                            Filename: filename,
                            Chunk:    chunk,
                        },
                    },
                }

                err := encoder.Encode(req)
                if err != nil {
                    log.Printf("Failed to send chunk %d: %v. Retries left: %d", chunk.Index, err, retries-1)
                    retries--
                    continue
                }

                var resp transport.Message
                err = decoder.Decode(&resp)
                if err != nil {
                    log.Printf("Failed to receive response for chunk %d: %v. Retries left: %d", chunk.Index, err, retries-1)
                    retries--
                    continue
                }

                if resp.Category == transport.MessageCategory_RESPONSE && resp.Operation == transport.MessageOperation_UPLOAD_CHUNK {
                    payload, ok := resp.Payload.(*transport.ResponsePayload)
                    if !ok || payload.UploadChunk == nil || !payload.UploadChunk.Success {
                        log.Printf("Upload failed for chunk %d. Retries left: %d", chunk.Index, retries-1)
                        retries--
                        continue
                    }
                    // 성공적으로 청크 전송
                    break
                }
            }

            if retries == 0 {
                errChan <- fmt.Errorf("failed to upload chunk %d after 3 retries", chunk.Index)
                return
            }
        }
        doneChan <- true
    }()

    // 청크 전송
    for _, chunk := range chunks {
        select {
        case chunkChan <- chunk:
        case err := <-errChan:
            close(chunkChan)
            return err
        case <-ctx.Done():
            close(chunkChan)
            return ctx.Err()
        }
    }

    close(chunkChan)

    // 모든 청크 전송 완료 대기
    select {
    case <-doneChan:
        return nil
    case err := <-errChan:
        return err
    case <-ctx.Done():
        return ctx.Err()
    }
}


func removeNode(nodes []*DataNodeInfo, nodeID string) []*DataNodeInfo {
    for i, node := range nodes {
        if node.ID == nodeID {
            return append(nodes[:i], nodes[i+1:]...)
        }
    }
    return nodes
}