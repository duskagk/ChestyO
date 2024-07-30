// test/upload_test.go

package test

import (
	"ChestyO/internal/enum"
	"ChestyO/internal/node"
	"ChestyO/internal/transport"
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

var file_name = "zxcv.txt"
var user_name = "TestUser"


func TestFileUpload(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()

    masterAddr := "localhost:8080"
    dataAddr1 := "localhost:8081"
    dataAddr2 := "localhost:8082"

    // MasterNode 시작
    master := node.NewMasterNode("master1")
    go func() {
        if err := master.Start(ctx, masterAddr); err != nil && err != context.Canceled {
            t.Errorf("Failed to run MasterNode: %v", err)
        }
    }()
    time.Sleep(time.Second)
    // DataNode 시작
    go func() {
        if err := node.RunDataNode(ctx, "data1", dataAddr1, masterAddr); err != nil && err != context.Canceled {
            t.Errorf("Failed to run DataNode1: %v", err)
        }
    }()

    go func() {
        if err := node.RunDataNode(ctx, "data2", dataAddr2, masterAddr); err != nil && err != context.Canceled {
            t.Errorf("Failed to run DataNode2: %v", err)
        }
    }()

    // DataNode가 모두 연결될 때까지 대기
    for i := 0; i < 30; i++ { // 최대 30초 대기
        if master.GetConnectedDataNodesCount() == 2 {
            break
        }
        time.Sleep(1 * time.Second)
    }
    if master.GetConnectedDataNodesCount() != 2 {
        t.Fatalf("Not all DataNodes connected. Expected 2, got %d", master.GetConnectedDataNodesCount())
    }

    content, err := os.ReadFile(file_name)
    if err != nil {
        t.Fatalf("Failed to read test file: %v", err)
    }

    // MasterNode에 연결
    conn, err := net.DialTimeout("tcp", masterAddr, 30*time.Second)
    if err != nil {
        t.Fatalf("Failed to connect to MasterNode: %v", err)
    }
    defer conn.Close()


    // 업로드 요청 전송
    err = transport.SendMessage(conn, &transport.Message{
        Type: transport.MessageType_UPLOAD,
        UploadRequest: &transport.UploadFileRequest{
            UserID:   user_name,
            Filename: file_name,
            FileSize: int64(len(content)),
            Policy:   enum.NoChange,
            Content:  content,
        },
    })
    if err != nil {
        t.Fatalf("Failed to send upload request: %v", err)
    }
    
    respChan := make(chan *transport.Message)
    errChan := make(chan error)

    go func() {
        resp, err := transport.ReceiveMessage(conn)
        if err != nil {
            errChan <- err
        } else {
            respChan <- resp
        }
    }()
    
    select {
    case resp := <-respChan:
        fmt.Printf("Receive : %v",resp)
        // 응답 처리
    case err := <-errChan:
        t.Fatalf("Failed to receive upload response: %v", err)
    case <-ctx.Done():
        t.Fatalf("Timeout or context cancelled while waiting for upload response")
    }
    cancel()
}

