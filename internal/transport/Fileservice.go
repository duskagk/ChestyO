package transport

import (
	"context"
	"io"
)

// transport/transport.go
type FileService interface {
	Register(ctx context.Context, req *RegisterMessage) error
    UploadFile(ctx context.Context, req *UploadFileRequest, createStream func() UploadStream) error
    UploadFileChunk(ctx context.Context, chunk *UploadFileChunk) error
	DownloadFile(ctx context.Context, req *DownloadFileRequest, stream DownloadStream) error
    DeleteFile(ctx context.Context, req *DeleteFileRequest) (*DeleteFileResponse, error)
    ListFiles(ctx context.Context, req *ListFilesRequest) (*ListFilesResponse, error)
    HasFile(ctx context.Context, userID, filename string) bool
}

// UploadStream represents a stream for uploading file chunks
type UploadStream interface {
    Send(*UploadFileChunk) error
    Recv() (*UploadFileChunk, error)
    CloseAndRecv() (*UploadFileResponse, error)
}

// DownloadStream represents a stream for downloading file chunks
type DownloadStream interface {
	Recv() (*FileChunk, error)
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

