// test/upload_test.go

package test

import (
	policy "ChestyO/internal/enum"
	"ChestyO/internal/node"
	"ChestyO/internal/transport"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var file_name = "zxcv.txt"
var user_name = "TestUser"


func TestNodeConnection(t *testing.T) {
    masterAddr := "localhost:8080"
    dataAddr1 := "localhost:8081"
    dataAddr2 := "localhost:8082"

    // Start MasterNode
    go func() {
        err := node.RunMasterNode("master1", masterAddr)
        if err != nil {
            t.Errorf("Failed to run MasterNode: %v", err)
        }
    }()

    // Give some time for MasterNode to start
    time.Sleep(1 * time.Second)

    // Start DataNode
    go func() {
        err := node.RunDataNode("data1", dataAddr1, masterAddr)
        if err != nil {
            t.Errorf("Failed to run DataNode: %v", err)
        }
    }()

    go func(){
        err:=node.RunDataNode("data2",dataAddr2, masterAddr)
        if err != nil{
            t.Errorf("Failed to run DataNode: %v", err)
        }
    }()

    // Give some time for DataNode to start and register
    time.Sleep(2 * time.Second)

    // Check if we can connect to MasterNode
    conn, err := net.Dial("tcp", masterAddr)
    if err != nil {
        t.Errorf("Failed to connect to MasterNode: %v", err)
    } else {
        conn.Close()
        log.Println("Successfully connected to MasterNode")
    }

    // Check if we can connect to DataNode
    conn, err = net.Dial("tcp", dataAddr1)
    
    if err != nil {
        t.Errorf("Failed to connect to DataNode: %v", err)
    } else {
        conn.Close()
        log.Println("Successfully connected to DataNode")
    }

    conn, err = net.Dial("tcp", dataAddr2)
    if err != nil {
        t.Errorf("Failed to connect to DataNode: %v", err)
    } else {
        conn.Close()
        log.Println("Successfully connected to DataNode")
    }
    // TODO: Add more specific tests for node registration and communication
}

func TestFileUpload(t *testing.T) {
    masterAddr := "localhost:8080"
    dataAddr1 := "localhost:8081"
    dataAddr2 := "localhost:8082"

    // Start MasterNode and DataNodes
    go func() {
        if err := node.RunMasterNode("master1", masterAddr); err != nil {
            t.Errorf("Failed to run MasterNode: %v", err)
        }
    }()
    time.Sleep(2 * time.Second)

    go func() {
        if err := node.RunDataNode("data1", dataAddr1, masterAddr); err != nil {
            t.Errorf("Failed to run DataNode1: %v", err)
        }
    }()

    go func() {
        if err := node.RunDataNode("data2", dataAddr2, masterAddr); err != nil {
            t.Errorf("Failed to run DataNode2: %v", err)
        }
    }()
    time.Sleep(10 * time.Second)

    log.Printf("Nodes started")

    // 파일 읽기

    content, err := ioutil.ReadFile(file_name)
    if err != nil {
        t.Fatalf("Failed to read test file: %v", err)
    }

    // MasterNode에 연결
    conn, err := net.Dial("tcp", masterAddr)
    if err != nil {
        t.Fatalf("Failed to connect to MasterNode: %v", err)
    }
    defer conn.Close()

    userID := "testuser"

    // 업로드 요청 전송
    err = transport.SendMessage(conn, &transport.Message{
        Type: transport.MessageType_UPLOAD,
        UploadRequest: &transport.UploadFileRequest{
            UserID:   userID,
            Filename: file_name,
            FileSize: int64(len(content)),
            Policy:   policy.NoChange,
        },
    })
    if err != nil {
        t.Fatalf("Failed to send upload request: %v", err)
    }
    time.Sleep(500 * time.Millisecond)

    // 파일 내용 전송
    err = transport.SendMessage(conn, &transport.Message{
        Type: transport.MessageType_UPLOAD,
        UploadChunk: &transport.FileChunk{
            Content: content,
            Index:   0,
        },
    })
    if err != nil {
        t.Fatalf("Failed to send file content: %v", err)
    }
    time.Sleep(500 * time.Millisecond)

    // EOF 신호 전송
    err = transport.SendMessage(conn, &transport.Message{
        Type: transport.MessageType_UPLOAD,
        UploadChunk: &transport.FileChunk{
            Content: nil,
            Index:   -1,
        },
    })
    if err != nil {
        t.Fatalf("Failed to send EOF signal: %v", err)
    }
    time.Sleep(500 * time.Millisecond)

    // 업로드 완료 응답 대기
    respMsg, err := transport.ReceiveMessage(conn)
    if err != nil {
        t.Fatalf("Failed to receive upload completion confirmation: %v", err)
    }

    if respMsg.Type != transport.MessageType_UPLOAD || respMsg.UploadResponse == nil {
        t.Fatalf("Unexpected response type or nil response")
    }

    if !respMsg.UploadResponse.Success {
        t.Fatalf("Upload failed: %s", respMsg.UploadResponse.Message)
    }

    log.Printf("Upload completed successfully: %s", respMsg.UploadResponse.Message)

    // 파일이 실제로 저장되었는지 확인
    time.Sleep(2 * time.Second) // 파일 시스템에 쓰기 위한 시간 여유
    expectedPaths := []string{
        filepath.Join("/tmp/datanode_data1", userID, file_name),
        filepath.Join("/tmp/datanode_data2", userID, file_name),
    }
    
    found := false
    for _, path := range expectedPaths {
        if _, err := os.Stat(path); err == nil {
            found = true
            log.Printf("File found at: %s", path)
            break
        }
    }

    if !found {
        t.Fatalf("File was not saved at any expected location")
    }
}