package transport

import (
	"context"
	"net"
)

// transport/transport.go
type FileService interface {
	TCPProtocl(ctx context.Context, conn net.Conn)
}

type MasterFileService interface{
	FileService
    Register(ctx context.Context, req *RegisterMessage) error
    UploadFile(ctx context.Context, req *UploadFileRequest) error
    DeleteFile(ctx context.Context, req *DeleteFileRequest) error
    // ListFiles(ctx context.Context, req *ListFilesRequest) (*ListFilesResponse, error)
    HasFile(ctx context.Context, userID, filename string) (bool, map[string]bool)
    DownloadFile(ctx context.Context, req *DownloadFileRequest) ([]byte,error)
}

type DataFileService interface {
    FileService
}


type TCPStream interface {
	Recv() (*Message, error)
    Send(*Message) error
	SendAndClose(*Message) error
    CloseAndRecv() (*Message, error)
    CloseStream()
}

