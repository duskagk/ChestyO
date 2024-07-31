package transport

import (
	"context"
)

// transport/transport.go
type FileService interface {
	Register(ctx context.Context, req *RegisterMessage) error
}

type MasterFileService interface{
	FileService
    UploadFile(ctx context.Context, req *UploadFileRequest) error
	// DownloadFile(ctx context.Context, req *DownloadFileRequest, stream DownloadStream) error
    DeleteFile(ctx context.Context, req *DeleteFileRequest) (*DeleteFileResponse, error)
    ListFiles(ctx context.Context, req *ListFilesRequest) (*ListFilesResponse, error)
    HasFile(ctx context.Context, userID, filename string) bool
}

type DataFileService interface {
    FileService
    UploadFileChunk(ctx context.Context, stream UploadStream) error
}


type UploadStream interface {
    Send(*UploadFileChunk) error
    CloseAndRecv() (*UploadFileResponse, error)
}

