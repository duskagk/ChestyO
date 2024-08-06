// test/upload_test.go

package test

import (
	"ChestyO/internal/enum"
	"ChestyO/internal/node"
	"ChestyO/internal/transport"
	"bytes"
	"context"
	"net"
	"os"
	"testing"
	"time"
)

var file_name = "zxcv.txt"
var user_name = "TestUser"


func TestFileOperations(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
    defer cancel()

    masterAddr := "localhost:8080"
    dataAddr1 := "localhost:8081"
    dataAddr2 := "localhost:8082"

    // MasterNode 시작
    master := node.NewMasterNode("master1", masterAddr, ":8090") // HTTP 주소 추가
    go func() {
        if err := master.Start(ctx); err != nil && err != context.Canceled {
            t.Errorf("Failed to run MasterNode: %v", err)
        }
    }()

    // DataNode 시작
    for _, addr := range []string{dataAddr1, dataAddr2} {
        go func(addr string) {
            if err := node.RunDataNode(ctx, "data-"+addr, addr, masterAddr); err != nil && err != context.Canceled {
                t.Errorf("Failed to run DataNode %s: %v", addr, err)
            }
        }(addr)
    }

    // 노드 연결 대기
    time.Sleep(2 * time.Second)
    for i := 0; i < 10; i++ {
        if master.GetConnectedDataNodesCount() == 2 {
            break
        }
        time.Sleep(time.Second)
    }

    if master.GetConnectedDataNodesCount() != 2 {
        t.Fatalf("Failed to connect all data nodes")
    }

    // 테스트 파일 준비
    fileName := "test.txt"
    userName := "testuser"
    content := []byte("Hello, World!")
    err := os.WriteFile(fileName, content, 0644)
    if err != nil {
        t.Fatalf("Failed to create test file: %v", err)
    }
    defer os.Remove(fileName)

    // 1. 파일 업로드 테스트
    t.Run("Upload", func(t *testing.T) {
        conn, err := net.Dial("tcp", masterAddr)
        if err != nil {
            t.Fatalf("Failed to connect to MasterNode: %v", err)
        }
        defer conn.Close()

        uploadReq := &transport.Message{
            Category:  transport.MessageCategory_REQUEST,
            Operation: transport.MessageOperation_UPLOAD,
            Payload: &transport.RequestPayload{
                Upload: &transport.UploadFileRequest{
                    UserID:   userName,
                    Filename: fileName,
                    FileSize: int64(len(content)),
                    Policy:   enum.NoChange,
                    Content:  content,
                },
            },
        }

        err = transport.SendMessage(conn, uploadReq)
        if err != nil {
            t.Fatalf("Failed to send upload request: %v", err)
        }

        response, err := transport.ReceiveMessage(conn)
        if err != nil {
            t.Fatalf("Failed to receive upload response: %v", err)
        }

        payload, ok := response.Payload.(*transport.ResponsePayload)
        if !ok || payload.Upload == nil {
            t.Fatalf("Invalid upload response")
        }

        if !payload.Upload.Success {
            t.Fatalf("Upload failed: %s", payload.Upload.Message)
        }
    })

    // 2. 파일 다운로드 테스트
    t.Run("Download", func(t *testing.T) {
        conn, err := net.Dial("tcp", masterAddr)
        if err != nil {
            t.Fatalf("Failed to connect to MasterNode: %v", err)
        }
        defer conn.Close()

        downloadReq := &transport.Message{
            Category:  transport.MessageCategory_REQUEST,
            Operation: transport.MessageOperation_DOWNLOAD,
            Payload: &transport.RequestPayload{
                Download: &transport.DownloadFileRequest{
                    UserID:   userName,
                    Filename: fileName,
                },
            },
        }

        err = transport.SendMessage(conn, downloadReq)
        if err != nil {
            t.Fatalf("Failed to send download request: %v", err)
        }

        response, err := transport.ReceiveMessage(conn)
        if err != nil {
            t.Fatalf("Failed to receive download response: %v", err)
        }

        payload, ok := response.Payload.(*transport.ResponsePayload)
        if !ok || payload.Download == nil {
            t.Fatalf("Invalid download response")
        }

        if !payload.Download.Success {
            t.Fatalf("Download failed: %s", payload.Download.Message)
        }

        if !bytes.Equal(payload.Download.FileContent, content) {
            t.Fatalf("Downloaded content does not match original content")
        }
    })

    // 3. 파일 삭제 테스트
    t.Run("Delete", func(t *testing.T) {
        conn, err := net.Dial("tcp", masterAddr)
        if err != nil {
            t.Fatalf("Failed to connect to MasterNode: %v", err)
        }
        defer conn.Close()

        deleteReq := &transport.Message{
            Category:  transport.MessageCategory_REQUEST,
            Operation: transport.MessageOperation_DELETE,
            Payload: &transport.RequestPayload{
                Delete: &transport.DeleteFileRequest{
                    UserID:   userName,
                    Filename: fileName,
                },
            },
        }

        err = transport.SendMessage(conn, deleteReq)
        if err != nil {
            t.Fatalf("Failed to send delete request: %v", err)
        }

        response, err := transport.ReceiveMessage(conn)
        if err != nil {
            t.Fatalf("Failed to receive delete response: %v", err)
        }

        payload, ok := response.Payload.(*transport.ResponsePayload)
        if !ok || payload.Delete == nil {
            t.Fatalf("Invalid delete response")
        }

        if !payload.Delete.Success {
            t.Fatalf("Delete failed: %s", payload.Delete.Message)
        }

        // 파일이 실제로 삭제되었는지 확인
        exists, _ := master.HasFile(ctx, userName, fileName)
        if exists {
            t.Fatalf("File still exists after deletion")
        }
    })
}