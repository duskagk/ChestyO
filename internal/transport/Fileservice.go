package transport

import (
	policy "ChestyO/internal/enum"
	"context"
	"io"
	"net"
)

// transport/transport.go
type FileService interface {
	UploadFile(ctx context.Context, req *UploadFileRequest, createStream func() UploadStream) error
	DownloadFile(ctx context.Context, req *DownloadFileRequest, stream DownloadStream) error
	DeleteFile(ctx context.Context, req *DeleteFileRequest) (*DeleteFileResponse, error)
	ListFiles(ctx context.Context, req *ListFilesRequest) (*ListFilesResponse, error)
}

// UploadStream represents a stream for uploading file chunks
type UploadStream interface {
	Send(*FileChunk) error
	Recv() (*FileChunk, error)
	CloseAndRecv() (*UploadFileResponse, error)
}

// DownloadStream represents a stream for downloading file chunks
type DownloadStream interface {
	Recv() (*FileChunk, error)
}

// Request and Response types
type UploadFileRequest struct {
	UserID    string
	ChunkName string
	Filename  string
	FileSize  int64
	Policy    policy.UploadPolicy
}

type UploadFileResponse struct {
	Success bool
	Message string
}

type DownloadFileRequest struct {
	Filename string
	UserID   string
}

type DeleteFileRequest struct {
	Filename string
	UserID   string
}

type DeleteFileResponse struct {
	Success bool
	Message string
}

type ListFilesRequest struct {
	Directory string
}

type ListFilesResponse struct {
	Files []FileInfo
}

type FileChunk struct {
	Content []byte
	Index   int
}

type FileInfo struct {
	Name  string
	Size  int64
	IsDir bool
}

type FileDownloadStream struct {
	chunks    [][]byte
	currIndex int
}

type FileMetadata struct {
	Filename    string
	Version     int
	ChunkMap    map[int]string // 청크 인덱스 -> 청크 파일 이름
	TotalChunks int
}

func NewFileDownloadStream() *FileDownloadStream {
	return &FileDownloadStream{
		chunks:    make([][]byte, 0),
		currIndex: 0,
	}
}

func (f *FileDownloadStream) Recv() (*FileChunk, error) {
	if f.currIndex >= len(f.chunks) {
		return nil, io.EOF
	}
	chunk := &FileChunk{Content: f.chunks[f.currIndex]}
	f.currIndex++
	return chunk, nil
}

func (f *FileDownloadStream) AddChunk(data []byte) {
	f.chunks = append(f.chunks, data)
}

type RegisterMessage struct{
	NodeID string
}

type tcpUploadStream struct {
    conn net.Conn
}