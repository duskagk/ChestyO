// test/upload_test.go

package test

import (
	"ChestyO/internal/enum"
	"ChestyO/internal/node"
	"ChestyO/internal/transport"
	"io/ioutil"
	"log"
	"net"
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
    conn.SetDeadline(time.Now().Add(30 * time.Second))
    if err != nil {
        t.Fatalf("Failed to connect to MasterNode: %v", err)
    }
    defer conn.Close()

    userID := "testuser"

    // 업로드 요청 전송
    transport.SendMessage(conn, &transport.Message{
        Type: transport.MessageType_UPLOAD,
        UploadRequest: &transport.UploadFileRequest{
            UserID:   userID,
            Filename: file_name,
            FileSize: int64(len(content)),
            Policy:   enum.NoChange,
            Content: content,
        },
    })
    if err != nil {
        t.Fatalf("Failed to send upload request: %v", err)
    }
    
    // 응답 대기
    resp, err := transport.ReceiveMessage(conn)
    if err != nil {
        t.Fatalf("Failed to receive upload response: %v", err)
    }
    
    // 응답 검증
    if resp.Type != transport.MessageType_UPLOAD || resp.UploadResponse == nil {
        t.Fatalf("Unexpected response type: %v", resp.Type)
    }
    
    if !resp.UploadResponse.Success {
        t.Fatalf("Upload failed: %s", resp.UploadResponse.Message)
    }
    
    log.Printf("File uploaded successfully")
}


func TestNodeConnections(t *testing.T) {
    masterAddr := "localhost:8080"
    dataAddr1 := "localhost:8081"
    dataAddr2 := "localhost:8082"

    // Start MasterNode
    go func() {
        if err := node.RunMasterNode("master1", masterAddr); err != nil {
            t.Errorf("Failed to run MasterNode: %v", err)
        }
    }()

    // Wait for MasterNode to start
    if !waitForServer(t, masterAddr) {
        t.Fatalf("MasterNode did not start")
    }
    log.Println("MasterNode started successfully")

    // Start DataNodes
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

    // Wait for DataNodes to start
    if !waitForServer(t, dataAddr1) {
        t.Fatalf("DataNode1 did not start")
    }
    log.Println("DataNode1 started successfully")

    if !waitForServer(t, dataAddr2) {
        t.Fatalf("DataNode2 did not start")
    }
    log.Println("DataNode2 started successfully")

    // Test connection to MasterNode
    conn, err := net.Dial("tcp", masterAddr)
    if err != nil {
        t.Fatalf("Failed to connect to MasterNode: %v", err)
    }
    conn.Close()
    log.Println("Successfully connected to MasterNode")

    // Give some time for DataNodes to register with MasterNode
    time.Sleep(2 * time.Second)

    // Test is complete
    log.Println("Connection test completed successfully")
}

func waitForServer(t *testing.T, addr string) bool {
    for i := 0; i < 10; i++ {
        conn, err := net.Dial("tcp", addr)
        if err == nil {
            conn.Close()
            return true
        }
        time.Sleep(time.Second)
    }
    return false
}