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
    HasFile(ctx context.Context, userID, filename string) (bool, *FileMetadata, error)
    DownloadFile(ctx context.Context, req *DownloadFileRequest) ([]*FileChunk, error)
    GetFileList(ctx context.Context, req *FileListRequest) (*ListFilesResponse, error)
    GetBuckets(ctx context.Context,limit, offset int)([]string,error)
    GetFileMetadata(ctx context.Context, bucket, filename string)(*FileMetadata, error)
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

