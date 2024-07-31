package transport

import (
	"ChestyO/internal/enum"
	"encoding/gob"
	"log"
	"net"
	"strings"
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
    MessageOperation_DELETE
    MessageOperation_LIST
    MessageOperation_UPLOAD_CHUNK
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
    Delete         *DeleteFileRequest
    List           *ListFilesRequest
    UploadChunk    *UploadFileChunk
}

type ResponsePayload struct {
    Register       *RegisterResponse
    Upload         *UploadFileResponse
    Download       *DownloadFileResponse
    Delete         *DeleteFileResponse
    List           *ListFilesResponse
    UploadChunk    *UploadChunkResponse
    Success        *SuccessResponse
    Error          *ErrorResponse
}


func SendMessage(conn net.Conn, msg *Message) error {
    log.Printf("SendMessage : %v\n",msg)
    encoder := gob.NewEncoder(conn)
    err := encoder.Encode(msg)
    if err != nil {
        log.Printf("Error encoding message: %v", err)
        if netErr, ok := err.(net.Error); ok {
            log.Printf("Network error: timeout=%v, temporary=%v", netErr.Timeout(), netErr.Temporary())
        }
    }
    return err
}

func ReceiveMessage(conn net.Conn) (*Message, error) {
    decoder := gob.NewDecoder(conn)
    msg := &Message{}
    err := decoder.Decode(msg)
    if err != nil {
        if strings.Contains(err.Error(), "duplicate type received") {
            // 중복 타입 에러 무시 또는 로깅
            log.Printf("Warning: Duplicate type received, ignoring: %v", err)
            return nil, nil
        }
        return nil, err
    }
    return msg, nil
}

// Request and Response types
type UploadFileRequest struct {
	UserID    string
	Filename  string
	FileSize  int64
	Policy    enum.UploadPolicy
    Content   []byte
}

type RegisterResponse struct {
    Success bool
	Message string
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


type UploadFileChunk struct {
	UserID    string
	Filename  string
    Chunk     FileChunk
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
    Exists bool
}

type UploadChunkResponse struct {
    Success    bool
    Message    string
    ChunkIndex int
}

type ErrorResponse struct {
    Message     string
}

type SuccessResponse struct{
    Message     string
}

func createErrorResponse(operation MessageOperation, errMsg string) *Message {
    return &Message{
        Category:  MessageCategory_RESPONSE,
        Operation: operation,
        Payload: &ResponsePayload{
            Error: &ErrorResponse{
                Message: errMsg,
            },
        },
    }
}

func createSuccessResponse(operation MessageOperation, msg string) *Message {
    return &Message{
        Category:  MessageCategory_RESPONSE,
        Operation: operation,
        Payload: &ResponsePayload{
            Success: &SuccessResponse{
                Message: msg,
            },
        },
    }
}


func init() {
    gob.Register(&Message{})
    gob.Register(&RequestPayload{})
    gob.Register(&ResponsePayload{})
    // gob.Register(&RegisterMessage{})
    // gob.Register(&UploadFileRequest{})
    // gob.Register(&DownloadFileRequest{})
    // gob.Register(&DeleteFileRequest{})
    // gob.Register(&ListFilesRequest{})
    // gob.Register(&UploadFileChunk{})
    // gob.Register(&RegisterResponse{})
    // gob.Register(&UploadFileResponse{})
    // gob.Register(&DownloadFileResponse{})
    // gob.Register(&DeleteFileResponse{})
    // gob.Register(&ListFilesResponse{})
    // gob.Register(&UploadChunkResponse{})
    // gob.Register(&SuccessResponse{})
    // gob.Register(&ErrorResponse{})
}