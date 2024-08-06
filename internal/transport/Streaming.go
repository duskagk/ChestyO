package transport

import (
	"encoding/gob"
	"net"
)

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
    s.conn.Close()
    return &msg, nil
}