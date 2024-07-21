package node

import (
	"ChestyO/internal/transport"
	"ChestyO/internal/utils"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
)

type MasterNode struct {
	ID            string
	dataNodes     map[string]*DataNode
	fileLocations map[string][]string
	transport     *transport.TCPTransport
	mu            sync.RWMutex
}

func NewMasterNode(id string) *MasterNode {
	return &MasterNode{
		ID:            id,
		dataNodes:     make(map[string]*DataNode),
		fileLocations: make(map[string][]string),
	}
}

func (m *MasterNode) HasFile(userId, filename string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, dataNode := range m.dataNodes {
		// 첫 번째 청크의 존재 여부만 확인
		// chunkFilename := fmt.Sprintf("%s_chunk_0", filename)
		if dataNode.HasFile(userId, filename) {
			return true
		}
	}

	return false
}

// node/master.go
func (m *MasterNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {

	fileExists := m.HasFile(req.UserID, req.Filename)

	if fileExists {
		log.Printf("File Already exist")
		return nil
		// switch req.Policy {
		// case policy.Overwrite:
		//     return m.handleOverwrite(ctx, req, createStream)
		// case policy.VersionControl:
		//     return m.handleVersionControl(ctx, req, createStream)
		// case policy.Deduplication:
		//     return m.handleDeduplication(ctx, req, createStream)
		// case policy.IncrementalUpdate:
		//     return m.handleIncrementalUpdate(ctx, req, createStream)
		// default:
		//     return fmt.Errorf("unsupported policy for existing file: %v", req.Policy)
		// }
	} else {
		// 새 파일 업로드
		return m.handleNewUpload(ctx, req, createStream)
	}
}

func (m *MasterNode) handleNewUpload(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {
	fmt.Printf("MasterNode: Starting file upload for %s\n", req.Filename)
	m.mu.Lock()
	defer m.mu.Unlock()

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

	dataNodes := make([]*DataNode, 0, len(m.dataNodes))
	for _, node := range m.dataNodes {
		dataNodes = append(dataNodes, node)
	}

	m.fileLocations[req.Filename] = make([]string, 0)
	// 각 노드에 청크 분배
	for i, chunk := range chunks {
		nodeIndex := i % len(dataNodes)
		dataNode := dataNodes[nodeIndex]

		err := dataNode.UploadFile(ctx, &transport.UploadFileRequest{
			Filename:  req.Filename,
			ChunkName: fmt.Sprintf("%s_chunk_%d", req.Filename, i),
			FileSize:  int64(len(chunk.Content)),
			UserID:    req.UserID,
		}, func() transport.UploadStream {
			return &transport.ChunkStream{Chunks: []transport.FileChunk{chunk}}
		})
		if err != nil {
			return fmt.Errorf("failed to upload chunk %d to node %s: %v", i, dataNode.ID, err)
		}
		if !contains(m.fileLocations[req.Filename], dataNode.ID) {
			m.fileLocations[req.Filename] = append(m.fileLocations[req.Filename], dataNode.ID)
		}

	}

	fmt.Printf("MasterNode: File upload completed for %s\n", req.Filename)
	return nil
}

func (m *MasterNode) handleOverwrite(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {
	// 기존 파일 삭제 후 새 파일 저장
	// m.deleteFile(req.Filename)
	// return m.saveFile(ctx, req, createStream)
	return nil
}

func (m *MasterNode) handleVersionControl(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {
	// 새 버전으로 파일 저장
	// version := m.getNextVersion(req.Filename)
	// req.Filename = fmt.Sprintf("%s_v%d", req.Filename, version)
	// return m.saveFile(ctx, req, createStream)
	return nil
}

func (m *MasterNode) handleDeduplication(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {
	// 파일 내용의 해시를 계산하여 중복 확인
	// hash, err := m.calculateFileHash(createStream)
	// if err != nil {
	//     return err
	// }
	// if existingFile := m.findFileByHash(hash); existingFile != "" {
	//     // 중복된 파일이 있으면 새 파일명으로 링크 생성
	//     return m.createLink(existingFile, req.Filename)
	// }
	// 중복이 없으면 새로 저장
	// return m.saveFile(ctx, req, createStream)
	return nil
}

func (m *MasterNode) handleIncrementalUpdate(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {
	// 기존 파일과 새 파일의 차이를 계산하여 변경된 부분만 업데이트
	// diff, err := m.calculateDiff(req.Filename, createStream)
	// if err != nil {
	//     return err
	// }
	// return m.applyDiff(req.Filename, diff)
	return nil
}

// func (m *MasterNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest, stream transport.DownloadStream) error {
//     m.mu.RLock()
//     defer m.mu.RUnlock()

//     log.Printf("Attempting to download file: %s for user: %s", req.Filename, req.UserId)

//     fds, ok := stream.(*transport.FileDownloadStream)
//     if !ok {
//         return fmt.Errorf("unsupported stream type")
//     }

//     fileFound := false
//     chunkIndex := 0

//     for {
//         chunkName := fmt.Sprintf("%s_chunk_%d", req.Filename, chunkIndex)
//         chunkFound := false

//         for _, dataNode := range m.dataNodes {
//             if dataNode.HasFile(req.UserId, req.Filename) {
//                 chunkData, err := dataNode.ReadChunk(req.UserId, req.Filename, chunkName)
//                 if err == nil {
//                     fds.AddChunk(chunkData)
//                     chunkFound = true
//                     fileFound = true
//                     break
//                 }
//                 log.Printf("Error reading chunk from node %s: %v", dataNode.ID, err)
//             }
//         }

//         if !chunkFound {
//             break
//         }

//         chunkIndex++
//     }

//     if !fileFound {
//         return fmt.Errorf("file not found: %s", req.Filename)
//     }

//     return nil
// }

func (m *MasterNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest, stream transport.DownloadStream) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	log.Printf("Attempting to download file: %s for user: %s", req.Filename, req.UserId)

	fds, ok := stream.(*transport.FileDownloadStream)
	if !ok {
		return fmt.Errorf("unsupported stream type")
	}

	allChunks := make(map[int][]byte)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, dataNode := range m.dataNodes {
		wg.Add(1)
		go func(node *DataNode) {
			defer wg.Done()
			chunks, err := node.ReadAllChunks(req.UserId, req.Filename)
			if err != nil {
				log.Printf("Error reading chunks from node %s: %v", node.ID, err)
				return
			}
			mu.Lock()
			for index, chunk := range chunks {
				allChunks[index] = chunk
			}
			mu.Unlock()
		}(dataNode)
	}

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

func (m *MasterNode) DeleteFile(ctx context.Context, req *transport.DeleteFileRequest) (*transport.DeleteFileResponse, error) {
	// 구현 로직
	return nil, nil
}

func (m *MasterNode) ListFiles(ctx context.Context, req *transport.ListFilesRequest) (*transport.ListFilesResponse, error) {
	// 구현 로직
	return nil, nil
}

func (m *MasterNode) Start(transport *transport.TCPTransport) error {
	m.transport = transport
	return m.transport.Serve()
}

func (m *MasterNode) Stop() {
	if m.transport != nil {
		m.transport.Close()
	}
}

func (m *MasterNode) AddDataNode(argDataName string, argNode *DataNode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dataNodes[argDataName] = argNode
}

func (m *MasterNode) selectDataNode() *DataNode {
	// 간단한 라운드 로빈 방식으로 데이터 노드 선택
	for _, node := range m.dataNodes {
		return node
	}
	return nil
}

func RunMasterNode(id, addr string) error {
	master := NewMasterNode(id)

	// TCP 전송 설정
	transport, err := transport.NewTCPTransport(addr, master)
	if err != nil {
		return fmt.Errorf("failed to set up TCP transport: %v", err)
	}

	fmt.Printf("Master node %s running on %s\n", id, addr)

	// 종료 신호를 받을 채널 생성
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// 마스터 노드 작업을 별도의 고루틴에서 실행
	go func() {
		if err := master.Start(transport); err != nil {
			fmt.Printf("Error in master node operations: %v\n", err)
			stop <- os.Interrupt // 오류 발생 시 종료 신호 전송
		}
	}()

	// 종료 신호를 기다림
	<-stop

	fmt.Println("Shutting down master node...")
	master.Stop() // 마스터 노드 정리 작업 수행

	return nil
}

func (m *MasterNode) sendChunkToStream(stream transport.DownloadStream, chunkData []byte) error {
	if fds, ok := stream.(*transport.FileDownloadStream); ok {
		fds.AddChunk(chunkData)
		return nil
	}
	return fmt.Errorf("unsupported stream type")
}

func contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}
