// test/upload_test.go

package test

import (
	"ChestyO/internal/enum"
	"ChestyO/internal/node"
	"ChestyO/internal/transport"
	"context"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

var file_name = "zxcv.txt"
var user_name = "TestUser"


func TestFileUpload(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(),time.Second*30)
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
    time.Sleep(time.Second)
    
    for i:=0;i<10;i++{
        if master.GetConnectedDataNodesCount()==2{
            break
        }
    }


    
    content, err := os.ReadFile(file_name)
    if err != nil {
        t.Fatalf("Failed to read test file: %v", err)
    }

    // MasterNode에 연결
    conn, err := net.Dial("tcp", masterAddr)
    if err != nil {
        t.Fatalf("Failed to connect to MasterNode: %v", err)
    }
    defer conn.Close()


    log.Printf("Send Upload request ...")
    // 업로드 요청 전송
    err = transport.SendMessage(conn, &transport.Message{
        Category :  transport.MessageCategory_REQUEST,
        Operation : transport.MessageOperation_UPLOAD,
            Payload  :  &transport.RequestPayload{
                Upload: &transport.UploadFileRequest{
                UserID:   user_name,
                Filename: file_name,
                FileSize: int64(len(content)),
                Policy:   enum.NoChange,
                Content:  content,
            },
        },
    })
    if err != nil {
        t.Fatalf("Failed to send upload request: %v", err)
    }

    // 응답 대기
    response, err := transport.ReceiveMessage(conn)
    if err != nil {
        t.Fatalf("Failed to receive upload response: %v", err)
    }

    // 응답 확인
    if err!=nil {
        t.Fatalf("Unexpected response type: %v", response)
    }

    // if !response.UploadResponse.Success {
    //     t.Fatalf("Upload failed: %s", response.UploadResponse.Message)
    // }

    log.Printf("Upload successful: %+v", response)
    conn.Close()
}

