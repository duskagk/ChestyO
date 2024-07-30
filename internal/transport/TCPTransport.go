package transport

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

type TCPTransport struct {
    listener net.Listener
    handler  FileService
}

func NewTCPTransport(address string, handler FileService) (*TCPTransport, error) {
    listener, err := net.Listen("tcp", address)
    if err != nil {
        return nil, err
    }
    return &TCPTransport{
        listener: listener,
        handler:  handler,
    }, nil
}

func (t *TCPTransport) Serve() error {
    for {
        conn, err := t.listener.Accept()
        if err != nil {
            return err
        }
        go t.handleConnection(conn)
    }
}

func (t *TCPTransport) Close() error {
    return t.listener.Close()
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
    defer conn.Close()
    decoder := gob.NewDecoder(conn)
    encoder := gob.NewEncoder(conn)
    var currentUploadRequest *UploadFileRequest
    var uploadStream UploadStream

    for {
        var msg Message
        log.Printf("Attempting to decode message...")
        err := decoder.Decode(&msg)
        if err != nil {
            if err != io.EOF {
                log.Printf("TCP : Error receiving message: %v %v", err, msg)
            }
            return
        }
        log.Printf("Successfully decoded message of type: %v", msg.Type)

        ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
        defer cancel()

        var respMsg *Message
        switch msg.Type {
        case MessageType_REGISTER:
            err = t.handler.Register(ctx,msg.RegisterMessage)
        case MessageType_UPLOAD:
            if msg.UploadRequest != nil {
                currentUploadRequest = msg.UploadRequest
                uploadStream = &tcpUploadStream{
                    conn:    conn,
                    decoder: decoder,
                    encoder: encoder,
                }
                err = t.handler.UploadFile(ctx, currentUploadRequest, func() UploadStream {
                    return uploadStream
                })
            } else if msg.UploadChunk != nil && currentUploadRequest != nil {
                // Handle chunk upload
                err = uploadStream.Send(msg.UploadChunk)
                if err == nil {
                    respMsg = &Message{
                        Type: MessageType_UPLOAD_CHUNK_RESPONSE,
                        UploadChunkResponse: &UploadChunkResponse{
                            Success: true,
                            Message: fmt.Sprintf("Chunk %d received successfully", msg.UploadChunk.Chunk.Index),
                            ChunkIndex: msg.UploadChunk.Chunk.Index,
                        },
                    }
                }
            } else {
                err = fmt.Errorf("invalid upload message or no active upload")
            }
        case MessageType_DOWNLOAD:
            err = t.handleDownload(ctx, conn, msg.DownloadRequest)
        case MessageType_DELETE:
            var resp *DeleteFileResponse
            resp, err = t.handler.DeleteFile(ctx, msg.DeleteRequest)
            if err == nil {
                respMsg = &Message{
                    Type:           MessageType_DELETE,
                    DeleteResponse: resp,
                }
            }
        case MessageType_LIST:
            var resp *ListFilesResponse
            resp, err = t.handler.ListFiles(ctx, msg.ListRequest)
            if err == nil {
                respMsg = &Message{
                    Type:         MessageType_LIST,
                    ListResponse: resp,
                }
            }
        case MessageType_UPLOAD_CHUNK:
            err = t.handler.UploadFileChunk(ctx, msg.UploadChunk)
            if err == nil {
                respMsg = &Message{
                    Type: MessageType_UPLOAD_CHUNK_RESPONSE,
                    UploadChunkResponse: &UploadChunkResponse{
                        Success: true,
                        Message: fmt.Sprintf("Chunk %d received successfully", msg.UploadChunk.Chunk.Index),
                        ChunkIndex: msg.UploadChunk.Chunk.Index,
                    },
                }
            }
        case MessageType_UPLOAD_CHUNK_RESPONSE:
            if msg.UploadChunkResponse !=nil{
                log.Printf("Received chunk upload response for index %d: %s", 
                msg.UploadChunkResponse.ChunkIndex, 
                msg.UploadChunkResponse.Message)
            }
        default:
            err = fmt.Errorf("unknown message type: %v", msg.Type)
        }

        if err != nil {
            log.Printf("Error handling message: %v", err)
            respMsg = &Message{
                Type: msg.Type,
                UploadResponse: &UploadFileResponse{
                    Success: false,
                    Message: err.Error(),
                },
            }
        }

        if respMsg != nil {
            log.Printf("TCP : SendMessage %v",respMsg)
            err = SendMessage(conn, respMsg)
            if err != nil {
                log.Printf("Error sending response: %v", err)
                return
            }
        }

        // Reset upload state if the upload is completed or an error occurred
        if msg.Type != MessageType_UPLOAD || err != nil {
            currentUploadRequest = nil
            uploadStream = nil
        }
    }
}

func (t *TCPTransport) handleUpload(ctx context.Context, conn net.Conn, req *UploadFileRequest) error {
    stream := &tcpUploadStream{
        conn:    conn,
        decoder: gob.NewDecoder(conn),
        encoder: gob.NewEncoder(conn),
    }
    return t.handler.UploadFile(ctx, req, func() UploadStream {
        return stream
    })
}

func (t *TCPTransport) handleDownload(ctx context.Context, conn net.Conn, req *DownloadFileRequest) error {
    stream := &tcpDownloadStream{
        conn:    conn,
        decoder: gob.NewDecoder(conn),
        encoder: gob.NewEncoder(conn),
    }
    return t.handler.DownloadFile(ctx, req, stream)
}

type tcpUploadStream struct {
    conn    net.Conn
    decoder *gob.Decoder
    encoder *gob.Encoder
}

func (s *tcpUploadStream) Send(chunk *UploadFileChunk) error {
    return s.encoder.Encode(chunk)
}

func (s *tcpUploadStream) Recv() (*UploadFileChunk, error) {
    log.Printf("Attempting to receive chunk")
    s.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
    var msg Message
    err := s.decoder.Decode(&msg)
    log.Printf("Decoded message: %+v, Error: %v", msg, err)
    s.conn.SetReadDeadline(time.Time{}) // 타임아웃 해제
    if err != nil {
        return nil, err
    }
    if msg.Type != MessageType_UPLOAD || msg.UploadChunk == nil {
        return nil, fmt.Errorf("unexpected message type or nil chunk")
    }
    return msg.UploadChunk, nil
}


func (s *tcpUploadStream) CloseAndRecv() (*UploadFileResponse, error) {
    msg := &Message{}
    err := s.decoder.Decode(msg)
    if err != nil {
        return nil, err
    }
    if msg.Type != MessageType_UPLOAD || msg.UploadResponse == nil {
        return nil, fmt.Errorf("unexpected message type or nil response")
    }
    return msg.UploadResponse, nil
}

type tcpDownloadStream struct {
    conn    net.Conn
    decoder *gob.Decoder
    encoder *gob.Encoder
}

func (s *tcpDownloadStream) Recv() (*FileChunk, error) {
    var chunk FileChunk
    err := s.decoder.Decode(&chunk)
    if err != nil {
        return nil, err
    }
    return &chunk, nil
}

func (s *tcpDownloadStream) Send(chunk *FileChunk) error {
    return s.encoder.Encode(chunk)
}