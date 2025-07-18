package transport

import (
	"ChestyO/internal/enum"
	"encoding/gob"
)

type MessageCategory int

const (
    MessageCategory_REQUEST MessageCategory = iota
    MessageCategory_RESPONSE
)

type MessageOperation int

const (
    MessageOperation_REGISTER MessageOperation = iota
    MessageOperation_UPLOAD
    MessageOperation_DOWNLOAD
    MessageOperation_HASFILE
    MessageOperation_DELETE
    MessageOperation_LIST
    MessageOperation_UPLOAD_CHUNK
    MessageOperation_DOWNLOAD_CHUNK
)

type Message struct {
    Category   MessageCategory
    Operation  MessageOperation
    Payload    interface{}
}

type RequestPayload struct {
    Register       *RegisterMessage
    Upload         *UploadFileRequest
    Download       *DownloadFileRequest
    HasFile        *HasFileRequest
    Delete         *DeleteFileRequest
    List           *ListFilesRequest
    UploadChunk    *UploadFileChunkRequest
    DownLoadChunk  *DownloadChunkRequest
}

type ResponsePayload struct {
    Register       *RegisterResponse
    Upload         *UploadFileResponse
    Download       *DownloadFileResponse
    HasFile        *HasFileResponse
    Delete         *DeleteFileResponse
    List           *ListFilesResponse
    UploadChunk    *UploadChunkResponse
    DownloadChunk  *DownloadChunkResponse
}

// Request and Response types
type UploadFileRequest struct {
	UserID    string
	Filename  string
	FileSize  int64
	Policy    enum.UploadPolicy
    Content   []byte
}

type BaseResponse struct {
    Success bool
    Message string
}

type RegisterResponse struct {
    BaseResponse
}

type UploadFileResponse struct {
    BaseResponse
}

type DownloadFileResponse struct {
    BaseResponse
    FileContent []byte
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
    BaseResponse
}

type ListFilesRequest struct {
	Directory string
}

type ListFilesResponse struct {
    BaseResponse
}


type UploadFileChunkRequest struct {
	UserID    string
	Filename  string
    Chunk     FileChunk
}

type DownloadChunkRequest struct{
    UserID     string
    Filename   string
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

type RegisterMessage struct{
	NodeID string
    Addr   string
}

type HasFileRequest struct {
    UserID   string
    Filename string
}

type HasFileResponse struct {
    BaseResponse
    IsExist  bool
}

type UploadChunkResponse struct {
    BaseResponse
    ChunkIndex int
}

type DownloadChunkResponse struct{
    BaseResponse
    Chunk FileChunk
}



func init() {
    gob.Register(&Message{})
    gob.Register(&RequestPayload{})
    gob.Register(&ResponsePayload{})
    gob.Register(&FileChunk{})
    gob.Register(UploadFileResponse{})
    gob.Register(UploadFileChunkRequest{})
}