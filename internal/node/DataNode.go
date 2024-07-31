package node

// internal/node/DataNode.go
import (
	"ChestyO/internal/store"
	"ChestyO/internal/transport"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type DataNode struct {
    ID         string
    store      *store.Store
    masterConn net.Conn
	stopChan chan struct{}
}

func NewDataNode(id string, storeOpts store.StoreOpts) *DataNode {
	return &DataNode{
		ID:    id,
		store: store.NewStore(storeOpts),
		stopChan: make(chan struct{}),
	}
}


func (d *DataNode) Start(ctx context.Context,addr string, masterAddr string) error {
    if err := d.RegisterWithMaster(addr, masterAddr); err != nil {
        return fmt.Errorf("failed to register with master: %v", err)
    }

    // transport, err := transport.NewTCPTransport(addr, d)
	transport, err := transport.NewDataTCPTransport(addr,d)
    if err != nil {
        return fmt.Errorf("failed to set up TCP transport: %v", err)
    }

    errChan := make(chan error, 1)
    go func() {
        errChan <- transport.Serve(ctx)
    }()

    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-d.stopChan:
        return nil
    case err := <-errChan:
        return err
    }
}

func (d * DataNode) Stop(){
	close(d.stopChan)
}

// node/data.go
func (d *DataNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {

	return nil;
}

func (d *DataNode) HasFile(ctx context.Context,userId, filename string) bool {
	return d.store.Has(userId, filename)
}

func RunDataNode(ctx context.Context,id, addr, masterAddr string) error {
    dataNode := NewDataNode(id, store.StoreOpts{Root: fmt.Sprintf("./tmp/datanode_%s", id)})
    return dataNode.Start(ctx,addr, masterAddr)
}

// DownloadFile downloads a file from the distributed system
func (d *DataNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest) error {
	return nil
}

// DeleteFile deletes a file from the distributed system
func (d *DataNode) DeleteFile(ctx context.Context, req *transport.DeleteFileRequest) (*transport.DeleteFileResponse, error) {
	log.Printf("DataNode %s: Attempting to delete file %s for user %s", d.ID, req.Filename, req.UserID)

	pathKey := d.store.PathTransformFunc(req.Filename)
	dirPath := filepath.Join(d.store.Root, req.UserID, pathKey.Pathname)

	// 디렉토리 내의 모든 청크 파일 삭제
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("DataNode %s: Directory not found for file %s", d.ID, req.Filename)
			return &transport.DeleteFileResponse{
				Success: false,
				Message: fmt.Sprintf("File %s not found", req.Filename),
			}, nil
		}
		return nil, fmt.Errorf("error reading directory: %v", err)
	}

	deletedCount := 0
	for _, file := range files {
		if strings.HasPrefix(file.Name(), req.Filename+"_chunk_") {
			err := os.Remove(filepath.Join(dirPath, file.Name()))
			if err != nil {
				log.Printf("DataNode %s: Error deleting chunk %s: %v", d.ID, file.Name(), err)
			} else {
				deletedCount++
			}
		}
	}

	// 빈 디렉토리 삭제
	err = os.Remove(dirPath)
	if err != nil && !os.IsNotExist(err) {
		log.Printf("DataNode %s: Warning: Could not delete empty directory %s: %v", d.ID, dirPath, err)
	}

	log.Printf("DataNode %s: Deleted %d chunks for file %s", d.ID, deletedCount, req.Filename)
	return &transport.DeleteFileResponse{
		Success: true,
		Message: fmt.Sprintf("File %s successfully deleted", req.Filename),
	}, nil
}

// ListFiles lists files in a directory
func (d *DataNode) ListFiles(ctx context.Context, req *transport.ListFilesRequest) (*transport.ListFilesResponse, error) {
	files, err := ioutil.ReadDir(filepath.Join(d.store.Root, d.ID))
	if err != nil {
		return nil, err
	}

	var fileInfos []transport.FileInfo
	for _, file := range files {
		if !file.IsDir() {
			fileInfos = append(fileInfos, transport.FileInfo{
				Name:  file.Name(),
				Size:  file.Size(),
				IsDir: false,
			})
		}
	}

	return &transport.ListFilesResponse{
		Files: fileInfos,
	}, nil
}

func (d *DataNode) GetFileList() ([]string, error) {
	dir := filepath.Join(d.store.Root, d.ID)
	absDir, _ := filepath.Abs(dir)
	fmt.Printf("GetFileList: Searching for files in %s\n", absDir)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var fileNames []string
	for _, file := range files {
		if !file.IsDir() {
			fileNames = append(fileNames, file.Name())
		}
	}

	return fileNames, nil
}

func (d *DataNode) ReadAllChunks(userId, filename string) (map[int][]byte, error) {
	fmt.Printf("DataNode %s: Attempting to read all chunks for file %s, user %s\n", d.ID, filename, userId)

	pathKey := d.store.PathTransformFunc(filename)
	dirPath := filepath.Join(d.store.Root, userId, pathKey.Pathname)

	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		fmt.Printf("DataNode %s: Error reading directory for file %s: %v\n", d.ID, filename, err)
		return nil, err
	}

	chunks := make(map[int][]byte)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), filename+"_chunk_") {
			chunkIndex, err := strconv.Atoi(strings.TrimPrefix(file.Name(), filename+"_chunk_"))
			if err != nil {
				fmt.Printf("DataNode %s: Error parsing chunk index for %s: %v\n", d.ID, file.Name(), err)
				continue
			}
			chunkData, err := d.ReadChunk(userId, filename, file.Name())
			if err != nil {
				fmt.Printf("DataNode %s: Error reading chunk %s: %v\n", d.ID, file.Name(), err)
				continue
			}
			chunks[chunkIndex] = chunkData
		}
	}

	fmt.Printf("DataNode %s: Successfully read %d chunks for file %s\n", d.ID, len(chunks), filename)
	return chunks, nil
}

func (d *DataNode) ReadChunk(userId, filename, chunkName string) ([]byte, error) {
	fmt.Printf("DataNode %s: Attempting to read chunk %s for user %s\n", d.ID, filename, userId)
	size, reader, err := d.store.Read(userId, filename, chunkName)
	if err != nil {
		fmt.Printf("DataNode %s: Error reading chunk %s: %v\n", d.ID, filename, err)
		return nil, err
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		fmt.Printf("DataNode %s: Error reading data from reader for chunk %s: %v\n", d.ID, filename, err)
		return nil, err
	}

	fmt.Printf("DataNode %s: Successfully read chunk %s. Size: %d bytes\n", d.ID, filename, size)
	return data, nil
}


func (d *DataNode) RegisterWithMaster(addr,masterAddr string) error {
    conn, err := net.Dial("tcp", masterAddr)
    if err != nil {
        return err
    }
    d.masterConn = conn

    msg := &transport.Message{
		Category: transport.MessageCategory_REQUEST,
		Operation:  transport.MessageOperation_REGISTER,
		Payload :  &transport.RequestPayload{
			Register: &transport.RegisterMessage{
				NodeID: d.ID,
				Addr: addr,
			},
		},
    }
    return transport.SendMessage(conn, msg)
}

func (m *DataNode) Register(ctx context.Context,req *transport.RegisterMessage) error {
    return nil
}


func (d *DataNode) UploadFileChunk(ctx context.Context,  stream transport.UploadStream) error{
	log.Printf("%v : data receive",d.ID)

    for {
        chunk, err := stream.Recv()
        if err == io.EOF {
            // 모든 청크를 받았음
            log.Printf("%v: Finished receiving all chunks", d.ID)
            return stream.SendAndClose(&transport.UploadChunkResponse{
                Success: true,
                Message: "All chunks received successfully",
            })
        }
        if err != nil {
            log.Printf("%v: Error receiving chunk: %v", d.ID, err)
            return err
        }

        // 청크 파일 이름 생성
        chunkFileName := fmt.Sprintf("%s_chunk_%d", chunk.Filename, chunk.Chunk.Index)

        // 청크 저장
        _, err = d.store.Write(chunk.UserID, chunk.Filename, chunkFileName, bytes.NewReader(chunk.Chunk.Content))
        if err != nil {
            log.Printf("%v: Error storing chunk %d: %v", d.ID, chunk.Chunk.Index, err)
            return stream.SendAndClose(&transport.UploadChunkResponse{
                Success: false,
                Message: fmt.Sprintf("Failed to store chunk %d: %v", chunk.Chunk.Index, err),
                ChunkIndex: chunk.Chunk.Index,
            })
        }

        log.Printf("%v: Successfully stored chunk %d of file %s for user %s", d.ID, chunk.Chunk.Index, chunk.Filename, chunk.UserID)

        // 각 청크 저장 성공 후 응답 전송 (선택적)
        err = stream.Send(&transport.UploadChunkResponse{
            Success: true,
            Message: fmt.Sprintf("Chunk %d uploaded successfully", chunk.Chunk.Index),
            ChunkIndex: chunk.Chunk.Index,
        })
        if err != nil {
            log.Printf("%v: Error sending response for chunk %d: %v", d.ID, chunk.Chunk.Index, err)
            return err
        }
    }

	// chunk_file_name := fmt.Sprintf("%s_chunk_%d", req.Filename, req.Chunk.Index)
	// _,err := d.store.Write(req.UserID,req.Filename,chunk_file_name,bytes.NewReader(req.Chunk.Content))
	

	// var response *transport.UploadChunkResponse;

	// log.Printf("%v : Store state %+v",d.ID, err)
	// if err ==nil{
	// 	response = &transport.UploadChunkResponse{
	// 		Success: true,
	// 		Message: fmt.Sprintf("Chunk %d uploaded successfully", req.Chunk.Index),
	// 		ChunkIndex: req.Chunk.Index,
	// 	}
	// }else{
	// 	response = &transport.UploadChunkResponse{
	// 		Success: false,
	// 		Message: fmt.Sprintf("Chunk %d uploaded successfully", req.Chunk.Index),
	// 		ChunkIndex: req.Chunk.Index,
	// 	}
	// }
	// log.Printf("%v : data send response %v",d.ID, response)

    return nil
}


