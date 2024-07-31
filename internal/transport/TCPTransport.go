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
            go t.handleConnection(ctx, conn)
        }
    }
}

func (t *TCPTransport) Close() error {
    close(t.stopChan)
    return t.listener.Close()
}

func (t *TCPTransport) handleConnection(ctx context.Context, conn net.Conn) {
    defer conn.Close()
    for {
        decoder := gob.NewDecoder(conn)
        // encoder := gob.NewEncoder(conn)
        // log.Printf("%v",encoder) // 에러 처리용
        select {
        case <-ctx.Done():
            log.Printf("Context cancelled, closing connection")
            return
        default:
            var msg Message
            log.Printf("Attempting to decode message...")
            err := decoder.Decode(&msg)
            if err != nil {
                if err == io.EOF {
                    log.Printf("Connection closed by client")
                    return
                }
                log.Printf("TCP : Error receiving message: %v", err)
                return
            }

            operationCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
            defer cancel() 

            if msg.Category == MessageCategory_REQUEST{
                switch msg.Operation{
                case MessageOperation_REGISTER:
                    
                    resp := t.handleRequestRegister(operationCtx, msg);
                    log.Printf("%v",resp)
                case MessageOperation_UPLOAD:
                    
                    if t.isMaster{
                        resp := t.handleRequestUploadFile(operationCtx, &msg)
                        log.Printf("%v",resp)
                    }
                case MessageOperation_UPLOAD_CHUNK:
                    log.Printf("Upload chunk Case")
                }
            }else if msg.Category == MessageCategory_RESPONSE{
                switch msg.Operation{
                }
            }
        }
        
    }
}

func (t *TCPTransport) handleRequestRegister(ctx context.Context, msg Message) *Message{
    log.Printf("Register start")
    payload ,ok := msg.Payload.(*RequestPayload)
    log.Printf("Payload : %v", msg.Payload)
    var err error
    if !ok {
        log.Printf("Payload is not of type *RequestPayload")
        return createErrorResponse(MessageOperation_REGISTER, "Invalid register payload type")
    }
    
    if payload.Register == nil {
        log.Printf("Register payload is nil")
        return createErrorResponse(MessageOperation_REGISTER, "Register payload is nil")
    }
    log.Printf("Register ...")
    if t.isMaster{
        err = t.masterHandler.Register(ctx,payload.Register);
    }else{
        err = t.dataHandler.Register(ctx, payload.Register)
    }
    if err != nil {
        return createErrorResponse(MessageOperation_REGISTER, err.Error())
    } else {
        return createSuccessResponse(MessageOperation_REGISTER, "Registration successful")
    }
}

func (t *TCPTransport) handleRequestUploadFile(ctx context.Context, msg *Message) *Message{
    payload ,ok := msg.Payload.(*RequestPayload)
    if !ok || payload.Upload == nil{
        return createErrorResponse(MessageOperation_UPLOAD, "Invalid register payload")
    }

    err := t.masterHandler.UploadFile(ctx, payload.Upload)

    if err !=nil{
        return createErrorResponse(MessageOperation_REGISTER, err.Error())
    }else{
        return createSuccessResponse(MessageOperation_UPLOAD, "Upload request successful")
    }
}


type TCPUploadStream struct {
    conn    net.Conn
    encoder *gob.Encoder
    decoder *gob.Decoder
}

func (s *TCPUploadStream) Send(chunk *UploadFileChunk) error {
    msg := &Message{
        Category:  MessageCategory_REQUEST,
        Operation: MessageOperation_UPLOAD_CHUNK,
        Payload: &RequestPayload{
            UploadChunk: chunk,
        },
    }
    return s.encoder.Encode(msg)
}

func (s *TCPUploadStream) CloseAndRecv() (*UploadChunkResponse, error) {
    // 스트림 종료 메시지 전송
    msg := &Message{
        Category:  MessageCategory_REQUEST,
        Operation: MessageOperation_UPLOAD,
        Payload: &RequestPayload{
            Upload: &UploadFileRequest{},
        },
    }
    err := s.encoder.Encode(msg)
    if err != nil {
        return nil, err
    }

    // 최종 응답 수신
    var response Message
    err = s.decoder.Decode(&response)
    if err != nil {
        return nil, err
    }

    if resp, ok := response.Payload.(*ResponsePayload); ok {
        return resp.UploadChunk, nil
    }
    return nil, fmt.Errorf("unexpected response type")
}
