package transport

import (
	policy "ChestyO/internal/enum"
	"encoding/gob"
	"net"
)

type MessageType int

const (
    MessageType_REGISTER MessageType = iota
    MessageType_UPLOAD
    MessageType_DOWNLOAD
    MessageType_DELETE
    MessageType_LIST
)

type Message struct {
    Type             MessageType
    RegisterMessage  *RegisterMessage
    UploadRequest    *UploadFileRequest
    DownloadRequest  *DownloadFileRequest
    DeleteRequest    *DeleteFileRequest
    ListRequest      *ListFilesRequest
    UploadChunk      *FileChunk
    DownloadChunk    *FileChunk
    UploadResponse   *UploadFileResponse
    DownloadResponse *DownloadFileResponse
    DeleteResponse   *DeleteFileResponse  // 추가됨
    ListResponse     *ListFilesResponse   // 추가됨
	ChunkRequest	 *FileChunk
}


func SendMessage(conn net.Conn, msg *Message) error {
    encoder := gob.NewEncoder(conn)
    return encoder.Encode(msg)
}

func ReceiveMessage(conn net.Conn) (*Message, error) {
    decoder := gob.NewDecoder(conn)
    msg := &Message{}
    err := decoder.Decode(msg)
    return msg, err
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

type DownloadFileResponse struct {
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
    Files   []FileInfo
    Success bool
    Message string
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
}

type HasFileRequest struct {
    UserID   string
    Filename string
}

type HasFileResponse struct {
    Exists bool
}