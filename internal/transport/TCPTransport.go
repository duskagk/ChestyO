package transport

import (
	"encoding/binary"
	"io"
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


// Close Connection
func (t *TCPTransport) Close() error{
	
	return nil
}


func (t *TCPTransport) handleConnection(conn net.Conn) {
    defer conn.Close()
    for {
        // Read message type
        var msgType uint32
        if err := binary.Read(conn, binary.BigEndian, &msgType); err != nil {
            return
        }

        // Handle different message types
        switch msgType {
        case 1: // UploadFile
            t.handleUploadFile(conn)
        case 2: // DownloadFile
            t.handleDownloadFile(conn)
        case 3: // DeleteFile
            t.handleDeleteFile(conn)
        case 4: // ListFiles
            t.handleListFiles(conn)
        default:
            return // Unknown message type
        }
    }
}

func (t *TCPTransport) handleUploadFile(conn net.Conn) {
    // Implement upload file logic
}

func (t *TCPTransport) handleDownloadFile(conn net.Conn) {
    // Implement download file logic
}

func (t *TCPTransport) handleDeleteFile(conn net.Conn) {
    // Implement delete file logic
}

func (t *TCPTransport) handleListFiles(conn net.Conn) {
    // Implement list files logic
}


// transport/transport.go

type ChunkStream struct {
    Chunks []FileChunk
    CurrentIndex int
}

func (cs *ChunkStream) Recv() (*FileChunk, error) {
    if cs.CurrentIndex >= len(cs.Chunks) {
        return nil, io.EOF
    }
    chunk := &cs.Chunks[cs.CurrentIndex]
    cs.CurrentIndex++
    return chunk, nil
}

func (cs *ChunkStream) Send(*FileChunk) error {
    return nil // 이 메서드는 사용되지 않지만 인터페이스 구현을 위해 필요합니다
}

func (cs *ChunkStream) CloseAndRecv() (*UploadFileResponse, error) {
    return &UploadFileResponse{Success: true}, nil
}