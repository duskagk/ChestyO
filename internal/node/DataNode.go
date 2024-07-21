package node

import (
	"ChestyO/internal/store"
	"ChestyO/internal/transport"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
)

type DataNode struct {
	ID    string
	store *store.Store
}

func NewDataNode(id string, storeOpts store.StoreOpts) *DataNode {
	return &DataNode{
		ID:    id,
		store: store.NewStore(storeOpts),
	}
}

// node/data.go

func (d *DataNode) UploadFile(ctx context.Context, req *transport.UploadFileRequest, createStream func() transport.UploadStream) error {
	fmt.Printf("DataNode %s: Starting file upload for %s\n", req.UserID, req.Filename)

	stream := createStream()
	var totalWritten int64 = 0
	chunkCount := 0

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving file chunk: %v", err)
		}

		n, err := d.store.Write(req.UserID, req.Filename, req.ChunkName, bytes.NewReader(chunk.Content))
		if err != nil {
			return fmt.Errorf("error writing file chunk: %v", err)
		}
		totalWritten += n
		chunkCount++
	}

	fmt.Printf("DataNode %s: File upload completed for %s. Wrote %d chunks, total size: %d\n", req.UserID, req.Filename, chunkCount, totalWritten)
	return nil
}

func (d *DataNode) HasFile(userId, filename string) bool {
	return d.store.Has(userId, filename)
}

func RunDataNode(id, addr, masterAddr string) error {
	dataNode := NewDataNode(id, store.StoreOpts{Root: fmt.Sprintf("/tmp/datanode_%s", id)})
	fmt.Printf("Data node %s running on %s, connected to master %s\n", id, addr, masterAddr)
	fmt.Print(dataNode.ID)
	select {} // Keep the node running
}

// DownloadFile downloads a file from the distributed system
func (d *DataNode) DownloadFile(ctx context.Context, req *transport.DownloadFileRequest, stream transport.DownloadStream) error {
	return nil
}

// DeleteFile deletes a file from the distributed system
func (d *DataNode) DeleteFile(ctx context.Context, req *transport.DeleteFileRequest) (*transport.DeleteFileResponse, error) {
	return nil, nil
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
