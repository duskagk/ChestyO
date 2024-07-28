package transport

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
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
    for {
        msg, err := ReceiveMessage(conn)
        if err != nil {
            if err != io.EOF {
                log.Printf("Error receiving message: %v", err)
            }
            return
        }

        switch msg.Type {
        case MessageType_REGISTER:
            err = t.handleRegister(conn, msg.RegisterMessage)
        case MessageType_UPLOAD:
            err = t.handleUpload(conn, msg.UploadRequest)
        case MessageType_DOWNLOAD:
            err = t.handleDownload(conn, msg.DownloadRequest)
        case MessageType_DELETE:
            err = t.handleDelete(conn, msg.DeleteRequest)
        case MessageType_LIST:
            err = t.handleList(conn, msg.ListRequest)
        default:
            err = fmt.Errorf("unknown message type: %v", msg.Type)
        }

        if err != nil {
            log.Printf("Error handling message: %v", err)
            return
        }
    }
}

func (t *TCPTransport) handleRegister(conn net.Conn, req *RegisterMessage) error {
    // Assuming FileService interface is extended to include Register method
    return t.handler.Register(context.Background(), req)
}

func (t *TCPTransport) handleUpload(conn net.Conn, req *UploadFileRequest) error {
    stream := &tcpUploadStream{conn: conn}
    return t.handler.UploadFile(context.Background(), req, func() UploadStream {
        return stream
    })
}

func (t *TCPTransport) handleDownload(conn net.Conn, req *DownloadFileRequest) error {
    stream := &tcpDownloadStream{conn: conn}
    return t.handler.DownloadFile(context.Background(), req, stream)
}

func (t *TCPTransport) handleDelete(conn net.Conn, req *DeleteFileRequest) error {
    resp, err := t.handler.DeleteFile(context.Background(), req)
    if err != nil {
        return err
    }
    return SendMessage(conn, &Message{
        Type:           MessageType_DELETE,
        DeleteResponse: resp,
    })
}

func (t *TCPTransport) handleList(conn net.Conn, req *ListFilesRequest) error {
    resp, err := t.handler.ListFiles(context.Background(), req)
    if err != nil {
        return err
    }
    return SendMessage(conn, &Message{
        Type:         MessageType_LIST,
        ListResponse: resp,
    })
}

type tcpUploadStream struct {
    conn net.Conn
}

func (s *tcpUploadStream) Send(chunk *FileChunk) error {
    return SendMessage(s.conn, &Message{
        Type:        MessageType_UPLOAD,
        UploadChunk: chunk,
    })
}

func (s *tcpUploadStream) Recv() (*FileChunk, error) {
    msg, err := ReceiveMessage(s.conn)
    if err != nil {
        return nil, err
    }
    if msg.Type != MessageType_UPLOAD {
        return nil, fmt.Errorf("unexpected message type: %v", msg.Type)
    }
    return msg.UploadChunk, nil
}

func (s *tcpUploadStream) CloseAndRecv() (*UploadFileResponse, error) {
    msg, err := ReceiveMessage(s.conn)
    if err != nil {
        return nil, err
    }
    if msg.Type != MessageType_UPLOAD {
        return nil, fmt.Errorf("unexpected message type: %v", msg.Type)
    }
    return msg.UploadResponse, nil
}

type tcpDownloadStream struct {
    conn net.Conn
}

func (s *tcpDownloadStream) Recv() (*FileChunk, error) {
    msg, err := ReceiveMessage(s.conn)
    if err != nil {
        return nil, err
    }
    if msg.Type != MessageType_DOWNLOAD {
        return nil, fmt.Errorf("unexpected message type: %v", msg.Type)
    }
    return msg.DownloadChunk, nil
}