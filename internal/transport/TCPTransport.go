package transport

import (
	"context"
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


