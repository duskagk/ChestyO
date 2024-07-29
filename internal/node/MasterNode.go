package node

// internal/node/MasterNode.go

import (
	policy "ChestyO/internal/enum"
	"ChestyO/internal/transport"
	"ChestyO/internal/utils"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

type DataNodeInfo struct {
    ID      string
    Address string
    Conn    net.Conn
}

type MasterNode struct {
    ID            string
    dataNodes     map[string]*DataNodeInfo
    fileLocations map[string][]string
    userMutexes   sync.Map
    transport     *transport.TCPTransport
    mu            sync.RWMutex
}

func NewMasterNode(id string) *MasterNode {
	return &MasterNode{
		ID:            id,
		dataNodes:     make(map[string]*DataNodeInfo),
		fileLocations: make(map[string][]string),
		userMutexes: sync.Map{},
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
    log.Printf("MasterNode: Starting file upload for %s\n", req.Filename)
    mu := m.getUserMutex(req.UserID)
    mu.Lock()
    defer mu.Unlock()

    stream := createStream()
    dataNodes := make([]*DataNodeInfo, 0, len(m.dataNodes))
    for _, node := range m.dataNodes {
        dataNodes = append(dataNodes, node)
    }

    m.fileLocations[req.Filename] = make([]string, 0)
    chunkIndex := 0

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            chunk, err := stream.Recv()
            if err != nil {
                if err == io.EOF {
                    log.Println("Received EOF, ending upload")
                    break
                }
                log.Printf("Error receiving chunk: %v", err)
                return fmt.Errorf("error receiving file chunk: %v", err)
            }

            nodeIndex := chunkIndex % len(dataNodes)
            dataNode := dataNodes[nodeIndex]

            err = m.sendChunkToDataNode(ctx, dataNode, req, chunk, chunkIndex)
            if err != nil {
                return fmt.Errorf("failed to send chunk to node %s: %v", dataNode.ID, err)
            }

            if !utils.Contains(m.fileLocations[req.Filename], dataNode.ID) {
                m.fileLocations[req.Filename] = append(m.fileLocations[req.Filename], dataNode.ID)
            }

            chunkIndex++
        }
    }

    log.Printf("MasterNode: File upload completed for %s\n", req.Filename)

    response, err := stream.CloseAndRecv()
    if err != nil {
        return fmt.Errorf("failed to send final response: %v", err)
    }
    if !response.Success {
        return fmt.Errorf("upload failed: %s", response.Message)
    }

    return nil
}


func (m *MasterNode) sendChunkToDataNode(ctx context.Context, dataNode *DataNodeInfo, req *transport.UploadFileRequest, chunk *transport.FileChunk, chunkIndex int) error {
    log.Printf("MasterNode: Starting to send chunk %d to DataNode %s at %v\n", chunkIndex, dataNode.ID, time.Now())

    uploadReq := &transport.UploadFileRequest{
        Filename:  req.Filename,
        ChunkName: fmt.Sprintf("%s_chunk_%d", req.Filename, chunkIndex),
        FileSize:  int64(len(chunk.Content)),
        UserID:    req.UserID,
    }

    msg := &transport.Message{
        Type:          transport.MessageType_UPLOAD,
        UploadRequest: uploadReq,
    }

    errChan := make(chan error, 1)
    go func() {
        log.Printf("MasterNode: Sending upload request for chunk %d to DataNode %s at %v\n", chunkIndex, dataNode.ID, time.Now())
        err := transport.SendMessage(dataNode.Conn, msg)
        if err != nil {
            log.Printf("MasterNode: Error sending upload request for chunk %d to DataNode %s at %v: %v\n", chunkIndex, dataNode.ID, time.Now(), err)
            errChan <- fmt.Errorf("failed to send upload request: %v", err)
            return
        }

        log.Printf("MasterNode: Sending chunk %d data to DataNode %s at %v\n", chunkIndex, dataNode.ID, time.Now())
        chunkMsg := &transport.Message{
            Type:        transport.MessageType_UPLOAD,
            UploadChunk: chunk,
        }
        err = transport.SendMessage(dataNode.Conn, chunkMsg)
        if err != nil {
            log.Printf("MasterNode: Error sending chunk %d data to DataNode %s at %v: %v\n", chunkIndex, dataNode.ID, time.Now(), err)
            errChan <- fmt.Errorf("failed to send chunk: %v", err)
            return
        }

        log.Printf("MasterNode: Waiting for response from DataNode %s for chunk %d at %v\n", dataNode.ID, chunkIndex, time.Now())
        respMsg, err := transport.ReceiveMessage(dataNode.Conn)
        if err != nil {
            log.Printf("MasterNode: Error receiving response from DataNode %s for chunk %d at %v: %v\n", dataNode.ID, chunkIndex, time.Now(), err)
            errChan <- fmt.Errorf("failed to receive response: %v", err)
            return
        }

        if respMsg.Type != transport.MessageType_UPLOAD || respMsg.UploadResponse == nil || !respMsg.UploadResponse.Success {
            log.Printf("MasterNode: Unexpected or failed response from DataNode %s for chunk %d at %v\n", dataNode.ID, chunkIndex, time.Now())
            errChan <- fmt.Errorf("unexpected or failed response from node")
            return
        }

        log.Printf("MasterNode: Successfully sent and received confirmation for chunk %d to DataNode %s at %v\n", chunkIndex, dataNode.ID, time.Now())
        errChan <- nil
    }()

    select {
    case err := <-errChan:
        if err != nil {
            log.Printf("MasterNode: Error sending chunk %d to DataNode %s at %v: %v\n", chunkIndex, dataNode.ID, time.Now(), err)
        } else {
            log.Printf("MasterNode: Successfully sent chunk %d to DataNode %s at %v\n", chunkIndex, dataNode.ID, time.Now())
        }
        return err
    case <-ctx.Done():
        log.Printf("MasterNode: Context cancelled while sending chunk %d to DataNode %s at %v\n", chunkIndex, dataNode.ID, time.Now())
        return ctx.Err()
    case <-time.After(10 * time.Second):
        log.Printf("MasterNode: Timeout sending chunk %d to DataNode %s at %v\n", chunkIndex, dataNode.ID, time.Now())
        return fmt.Errorf("timeout sending chunk to node %s", dataNode.ID)
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

func (m *MasterNode) Start(addr string) error {
    transport, err := transport.NewTCPTransport(addr, m)
    if err != nil {
        return fmt.Errorf("failed to set up TCP transport: %v", err)
    }
    m.transport = transport

    log.Printf("Master node %s running on %s", m.ID, addr)
    return m.transport.Serve()
}

func (m *MasterNode) Stop() {
	if m.transport != nil {
		m.transport.Close()
	}
}


func RunMasterNode(id, addr string) error {
    master := NewMasterNode(id)
    return master.Start(addr)
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

func (m *MasterNode) handleRegister(conn net.Conn, msg *transport.RegisterMessage) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    m.dataNodes[msg.NodeID] = &DataNodeInfo{
        ID:      msg.NodeID,
        Address: conn.RemoteAddr().String(),
        Conn:    conn,
    }

    log.Printf("Registered DataNode: %s at %s", msg.NodeID, conn.RemoteAddr().String())
    return nil
}

func (m *MasterNode) Register(ctx context.Context,conn net.Conn, req *transport.RegisterMessage) error {
    return m.handleRegister(conn, req)
}