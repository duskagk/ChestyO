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
    // masterConn net.Conn
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
	transport, err := transport.NewTCPTransport(addr,d)
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


func (d *DataNode) TCPProtocl(ctx context.Context, conn net.Conn) {
    log.Printf("DataNode %s: Starting TCP protocol", d.ID)
    // defer conn.Close()

    stream := transport.NewTCPStream(conn)

    for {
        select {
        case <-ctx.Done():
            log.Printf("DataNode %s: Context cancelled, closing connection", d.ID)
            return
        default:
            msg, err := stream.Recv()
            if err != nil {
                if err == io.EOF {
                    log.Printf("DataNode %s: Connection closed by peer", d.ID)
                    return
                }
                log.Printf("DataNode %s: Error receiving message: %v", d.ID, err)
                return
            }

            log.Printf("DataNode %s: Received message: Category=%v, Operation=%v", d.ID, msg.Category, msg.Operation)

            switch msg.Operation {
            case transport.MessageOperation_UPLOAD_CHUNK:
                err = d.handleUploadChunks(ctx, stream, msg)
			case transport.MessageOperation_DOWNLOAD_CHUNK:
				err = d.handleDownloadChunks(ctx,stream, msg)
			case transport.MessageOperation_HASFILE:
				err = d.handleHasFile(ctx, stream, msg)
				if err != nil {
					log.Printf("DataNode %s: Error handling HasFile: %v", d.ID, err)
					return
				}
				return
			case transport.MessageOperation_DELETE:
				err = d.handleDeleteFile(ctx ,stream,msg)
				if err != nil{
					log.Printf("DataNode %s: Error handling Delet : %v", d.ID,err)
					return
				}
				return
            default:
                log.Printf("DataNode %s: Unknown operation: %v", d.ID, msg.Operation)
                err = fmt.Errorf("unknown operation")
            }

            if err != nil {
                log.Printf("DataNode %s: Error handling operation: %v", d.ID, err)
                // 에러 응답 보내기
                errMsg := &transport.Message{
                    Category:  transport.MessageCategory_RESPONSE,
                    Operation: msg.Operation,
                    Payload: &transport.ResponsePayload{
                        UploadChunk: &transport.UploadChunkResponse{
							BaseResponse: transport.BaseResponse{
								Success: false,
								Message: err.Error(),
							},
                        },
                    },
                }
                if sendErr := stream.Send(errMsg); sendErr != nil {
                    log.Printf("DataNode %s: Error sending error response: %v", d.ID, sendErr)
                }
                return
            }
        }
    }
}

func (d *DataNode) handleDownloadChunks(ctx context.Context, stream transport.TCPStream, msg *transport.Message) error {
    payload, ok := msg.Payload.(*transport.RequestPayload)
    if !ok || payload.DownLoadChunk == nil {
        return fmt.Errorf("invalid payload for download chunk")
    }

    req := payload.DownLoadChunk
    log.Printf("DataNode %s: Processing download request for file %s, user %s", d.ID, req.Filename, req.UserID)

    chunks, err := d.ReadAllChunks(req.UserID, req.Filename)
    if err != nil {
        return fmt.Errorf("failed to read chunks: %v", err)
    }

    for index, content := range chunks {
        response := &transport.Message{
            Category:  transport.MessageCategory_RESPONSE,
            Operation: transport.MessageOperation_DOWNLOAD_CHUNK,
            Payload: &transport.ResponsePayload{
                DownloadChunk: &transport.DownloadChunkResponse{
                    Chunk: transport.FileChunk{
                        Index:   index,
                        Content: content,
                    },
                },
            },
        }
        if err := stream.Send(response); err != nil {
            return fmt.Errorf("failed to send chunk response: %v", err)
        }
    }

    // 모든 청크를 전송한 후 스트림 종료
    return stream.SendAndClose(&transport.Message{
        Category:  transport.MessageCategory_RESPONSE,
        Operation: transport.MessageOperation_DOWNLOAD_CHUNK,
        Payload: &transport.ResponsePayload{
            DownloadChunk: &transport.DownloadChunkResponse{
                BaseResponse: transport.BaseResponse{
                    Success: true,
                    Message: "All chunks sent",
                },
            },
        },
    })
}


func (d *DataNode) handleUploadChunks(ctx context.Context, stream transport.TCPStream, msg *transport.Message) error {
    payload, ok := msg.Payload.(*transport.RequestPayload)
    if !ok || payload.UploadChunk == nil {
        return fmt.Errorf("invalid payload for upload chunk")
    }

    chunk := payload.UploadChunk
    log.Printf("DataNode %s: Processing chunk %d for file %s", d.ID, chunk.Chunk.Index, chunk.Filename)

    // Store the chunk
    chunkFileName := fmt.Sprintf("%s_chunk_%d", chunk.Filename, chunk.Chunk.Index)
    _, err := d.store.Write(chunk.UserID, chunk.Filename, chunkFileName, bytes.NewReader(chunk.Chunk.Content))
    if err != nil {
        return fmt.Errorf("failed to store chunk: %v", err)
    }

    // Send success response
    response := &transport.Message{
        Category:  transport.MessageCategory_RESPONSE,
        Operation: transport.MessageOperation_UPLOAD_CHUNK,
        Payload: &transport.ResponsePayload{
            UploadChunk: &transport.UploadChunkResponse{
				BaseResponse: transport.BaseResponse{
					Success: true,
					Message: fmt.Sprintf("Chunk %d stored successfully", chunk.Chunk.Index),
				},
				ChunkIndex: payload.UploadChunk.Chunk.Index,
            },
			
        },
    }
    if err := stream.Send(response); err != nil {
        return fmt.Errorf("failed to send response: %v", err)
    }

    return nil
}

func (d *DataNode) handleHasFile(ctx context.Context, stream transport.TCPStream, msg *transport.Message) error {
    payload, ok := msg.Payload.(*transport.RequestPayload)

    if !ok || payload.HasFile == nil {
        response := &transport.Message{
            Category:  transport.MessageCategory_RESPONSE,
            Operation: transport.MessageOperation_HASFILE,  // 수정됨
            Payload: &transport.ResponsePayload{
                HasFile: &transport.HasFileResponse{
                    BaseResponse: transport.BaseResponse{
                        Success: false,
                        Message: "Response not valid",
                    },
                    IsExist: false,
                },
            },
        }
        return stream.SendAndClose(response)
    }
    is_exist := d.HasFile(ctx, payload.HasFile.UserID, payload.HasFile.Filename)  // Filename으로 수정
    response := &transport.Message{
        Category:  transport.MessageCategory_RESPONSE,
        Operation: transport.MessageOperation_HASFILE,  // 수정됨
        Payload: &transport.ResponsePayload{
            HasFile: &transport.HasFileResponse{
                BaseResponse: transport.BaseResponse{
                    Success: true,
                    Message: "Find file result",
                },
                IsExist: is_exist,
            },
        },
    }
    return stream.SendAndClose(response)
}

func (d *DataNode) handleDeleteFile(ctx context.Context, stream transport.TCPStream, msg *transport.Message) error {
	log.Printf("Node %s : handle delete file start",d.ID)
    payload, ok := msg.Payload.(*transport.RequestPayload)
    
    if !ok || payload.Delete == nil {
        response := &transport.Message{
            Category:  transport.MessageCategory_RESPONSE,
            Operation: transport.MessageOperation_DELETE,
            Payload: &transport.ResponsePayload{
                Delete: &transport.DeleteFileResponse{
                    BaseResponse: transport.BaseResponse{
                        Success: false,
                        Message: "Invalid request payload",
                    },
                },
            },
        }
        if err := stream.Send(response); err != nil {
            return fmt.Errorf("failed to send error response: %v", err)
        }
        return fmt.Errorf("invalid request payload")
    }

    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
        err := d.store.Delete(payload.Delete.UserID, payload.Delete.Filename)

        success := err == nil
        message := "Delete successful"
        if err != nil {
            message = fmt.Sprintf("Delete failed: %v", err)
        }

        response := &transport.Message{
            Category:  transport.MessageCategory_RESPONSE,
            Operation: transport.MessageOperation_DELETE,
            Payload: &transport.ResponsePayload{
                Delete: &transport.DeleteFileResponse{
                    BaseResponse: transport.BaseResponse{
                        Success: success,
                        Message: message,
                    },
                },
            },
        }

        if sendErr := stream.SendAndClose(response); sendErr != nil {
            return fmt.Errorf("failed to send response: %v", sendErr)
        }

        if success {
            log.Printf("DataNode: Deleted file %s for user %s", payload.Delete.Filename, payload.Delete.UserID)
        } else {
            log.Printf("DataNode: Failed to delete file %s for user %s: %v", payload.Delete.Filename, payload.Delete.UserID, err)
        }

        return err
    }
}



func (d *DataNode) HasFile(ctx context.Context,userId, filename string) bool {
	return d.store.Has(userId, filename)
}

func RunDataNode(ctx context.Context,id, addr, masterAddr string) error {
    dataNode := NewDataNode(id, store.StoreOpts{Root: fmt.Sprintf("./tmp/datanode_%s", id)})
    return dataNode.Start(ctx,addr, masterAddr)
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
				BaseResponse: transport.BaseResponse{
					Success: false,
					Message: fmt.Sprintf("File %s not found", req.Filename),
				},
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
		BaseResponse: transport.BaseResponse{
			Success: true,
			Message: fmt.Sprintf("File %s successfully deleted", req.Filename),
		},
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
    // d.masterConn = conn

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




