package transport

import (
	"context"
	"encoding/gob"
	"log"
	"net"
	"strings"
	"time"
)

type TCPTransport struct {
    listener net.Listener
    handler  FileService
    stopChan chan struct{}
}

func NewTCPTransport(address string, handler FileService) (*TCPTransport, error) {
    listener, err := net.Listen("tcp", address)
    if err != nil {
        return nil, err
    }
    return &TCPTransport{
        listener: listener,
        handler:  handler,
        stopChan: make(chan struct{}),
    }, nil
}

func (t *TCPTransport) Serve(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-t.stopChan:
            return nil
        default:
            conn, err := t.listener.Accept()
            if err != nil {
                if ne, ok := err.(net.Error); ok && ne.Temporary() {
                    time.Sleep(time.Second)
                    continue
                }
                return err
            }
            go t.handler.TCPProtocl(ctx,conn)
        }
    }
}

func (t *TCPTransport) Close() error {
    close(t.stopChan)
    return t.listener.Close()
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