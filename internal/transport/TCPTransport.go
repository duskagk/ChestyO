package transport

import (
	"context"
	"encoding/gob"
	"net"
	"time"
)

type TCPTransport struct {
    listener net.Listener
    masterHandler MasterFileService
    dataHandler DataFileService
    stopChan chan struct{}
    isMaster bool
}

func NewMasterTCPTransport(address string, handler MasterFileService) (*TCPTransport, error) {
    listener, err := net.Listen("tcp", address)
    if err != nil {
        return nil, err
    }
    return &TCPTransport{
        listener: listener,
        masterHandler: handler,
        stopChan: make(chan struct{}),
        isMaster: true,
    }, nil
}

func NewDataTCPTransport(address string, handler DataFileService) (*TCPTransport, error) {
    listener, err := net.Listen("tcp", address)
    if err != nil {
        return nil, err
    }
    return &TCPTransport{
        listener: listener,
        dataHandler: handler,
        stopChan: make(chan struct{}),
        isMaster: false,
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
            if t.isMaster{
                go t.masterHandler.TCPProtocl(ctx,conn);
            }else{
                go t.dataHandler.TCPProtocl(ctx,conn)
            }
        }
    }
}

func (t *TCPTransport) Close() error {
    close(t.stopChan)
    return t.listener.Close()
}


// TCPUpload Stream
type StreamService struct {
    conn    net.Conn
    encoder *gob.Encoder
    decoder *gob.Decoder
}

func NewTCPStream(conn net.Conn) *StreamService {
    return &StreamService{
        conn:    conn,
        encoder: gob.NewEncoder(conn),
        decoder: gob.NewDecoder(conn),
    }
}


func (s *StreamService) Recv() (*Message, error) {
    var msg Message
    err := s.decoder.Decode(&msg)
    if err != nil {
        return nil, err
    }
    return &msg, nil
}

func (s *StreamService) Send(req *Message) error {
    return s.encoder.Encode(req)
}

func (s *StreamService) SendAndClose(res *Message) error {
    err := s.encoder.Encode(res)
    if err != nil {
        return err
    }
    return s.conn.Close()
}

func (s *StreamService) CloseAndRecv() (*Message, error) {
    var msg Message
    err := s.decoder.Decode(&msg)
    if err != nil {
        return nil, err
    }
    return &msg, nil
}