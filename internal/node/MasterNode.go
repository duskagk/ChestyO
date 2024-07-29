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
    fmt.Printf("MasterNode: Starting file upload for %s\n", req.Filename)
    mu := m.getUserMutex(req.UserID)
    mu.Lock()
    defer mu.Unlock()

    stream := createStream()
    var fileContent []byte
    for {
        chunk, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            return fmt.Errorf("error receiving file chunk: %v", err)
        }
        fileContent = append(fileContent, chunk.Content...)
    }

    chunks := utils.SplitFileIntoChunks(fileContent)
    fmt.Printf("MasterNode: File split into %d chunks\n", len(chunks))

    dataNodes := make([]*DataNodeInfo, 0, len(m.dataNodes))
    for _, node := range m.dataNodes {
        dataNodes = append(dataNodes, node)
    }

    m.fileLocations[req.Filename] = make([]string, 0)
    // 각 노드에 청크 분배
    for i, chunk := range chunks {
        nodeIndex := i % len(dataNodes)
        dataNode := dataNodes[nodeIndex]

        uploadReq := &transport.UploadFileRequest{
            Filename:  req.Filename,
            ChunkName: fmt.Sprintf("%s_chunk_%d", req.Filename, i),
            FileSize:  int64(len(chunk.Content)),
            UserID:    req.UserID,
        }

        msg := &transport.Message{
            Type:          transport.MessageType_UPLOAD,
            UploadRequest: uploadReq,
        }

        err := transport.SendMessage(dataNode.Conn, msg)
        if err != nil {
            return fmt.Errorf("failed to send upload request to node %s: %v", dataNode.ID, err)
        }

        // Send the chunk
        chunkMsg := &transport.Message{
            Type:       transport.MessageType_UPLOAD,
            // UploadChunk: &transport.FileChunk{Content: chunk.Content, Index: i},
			ChunkRequest: &transport.FileChunk{
				Content: chunk.Content,
				Index: i,
			},
        }
        err = transport.SendMessage(dataNode.Conn, chunkMsg)
        if err != nil {
            return fmt.Errorf("failed to send chunk to node %s: %v", dataNode.ID, err)
        }

        // Receive response
        respMsg, err := transport.ReceiveMessage(dataNode.Conn)
        if err != nil {
            return fmt.Errorf("failed to receive response from node %s: %v", dataNode.ID, err)
        }

        if respMsg.Type != transport.MessageType_UPLOAD  {
            return fmt.Errorf("unexpected response from node %s", dataNode.ID)
        }

        // if !respMsg.UploadResponse.Success {
        //     return fmt.Errorf("failed to upload chunk %d to node %s: %s", i, dataNode.ID, respMsg.UploadResponse.Message)
        // }

        if !utils.Contains(m.fileLocations[req.Filename], dataNode.ID) {
            m.fileLocations[req.Filename] = append(m.fileLocations[req.Filename], dataNode.ID)
        }
    }

    fmt.Printf("MasterNode: File upload completed for %s\n", req.Filename)
    return nil
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