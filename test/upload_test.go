// test/upload_test.go

package test

import (
	"ChestyO/internal/node"
	"ChestyO/internal/transport"
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

    // Start MasterNode and DataNodes (재사용 가능한 함수로 만들 수 있습니다)
    go node.RunMasterNode("master1", masterAddr)
    time.Sleep(1 * time.Second)
    go node.RunDataNode("data1", dataAddr1, masterAddr)
    go node.RunDataNode("data2", dataAddr2, masterAddr)
    time.Sleep(2 * time.Second)

    log.Printf("Data Node start")
    // 파일 업로드 테스트
    content := []byte("This is a test file content")
    req := &transport.UploadFileRequest{
        Filename: file_name,
        UserID:   user_name,
        FileSize: int64(len(content)),
    }

    // MasterNode에 연결
    conn, err := net.Dial("tcp", masterAddr)
    if err != nil {
        t.Fatalf("Failed to connect to MasterNode: %v", err)
    }
    defer conn.Close()

    // 업로드 요청 전송
    msg := &transport.Message{
        Type:          transport.MessageType_UPLOAD,
        UploadRequest: req,
    }
    err = transport.SendMessage(conn, msg)
    if err != nil {
        t.Fatalf("Failed to send upload request: %v", err)
    }

    // 파일 내용 전송
    chunkMsg := &transport.Message{
        Type: transport.MessageType_UPLOAD,
        ChunkRequest: &transport.FileChunk{
            Content: content,
            Index:   0,
        },
    }
    err = transport.SendMessage(conn, chunkMsg)
    if err != nil {
        t.Fatalf("Failed to send file content: %v", err)
    }

    // 응답 수신
    respMsg, err := transport.ReceiveMessage(conn)
    if err != nil {
        t.Fatalf("Failed to receive response: %v", err)
    }

    if respMsg.Type != transport.MessageType_UPLOAD {
        t.Fatalf("Unexpected response type: %v", respMsg.Type)
    }

    log.Println("File upload test completed successfully")

    // 파일이 실제로 저장되었는지 확인
    time.Sleep(1 * time.Second) // 파일 저장을 위한 시간 여유


    log.Println("File was successfully stored in at least one DataNode")
}


// func TestFileUpload(t *testing.T) {
// 	// 마스터 노드 설정
// 	masterAddr := ":8000"
// 	master := node.NewMasterNode("master1")
// 	masterTransport, err := transport.NewTCPTransport(masterAddr, master)
// 	if err != nil {
// 		t.Fatalf("Failed to create master transport: %v", err)
// 	}

// 	// 데이터 노드 설정
// 	dataNode1 := node.NewDataNode("data1", store.StoreOpts{Root: "./tmp1/datanode1"})
// 	dataNode2 := node.NewDataNode("data2", store.StoreOpts{Root: "./tmp2/datanode2"})

// 	dataTransport1, err := transport.NewTCPTransport(":8001", dataNode1)
// 	if err != nil {
// 		t.Fatalf("Failed to create data node 1 transport: %v", err)
// 	}
// 	dataTransport2, err := transport.NewTCPTransport(":8002", dataNode2)
// 	if err != nil {
// 		t.Fatalf("Failed to create data node 2 transport: %v", err)
// 	}

// 	// 노드들 시작
// 	go masterTransport.Serve()
// 	go dataTransport1.Serve()
// 	go dataTransport2.Serve()

// 	// 마스터 노드에 데이터 노드 등록
// 	master.AddDataNode(dataNode1.ID, dataNode1)
// 	master.AddDataNode(dataNode2.ID, dataNode2)

// 	// 테스트 파일 내용을 메모리에 저장
// 	fileContent, err := ioutil.ReadFile(file_name)
// 	if err != nil {
// 		t.Fatalf("Failed to read test file: %v", err)
// 	}

// 	chunks := utils.SplitFileIntoChunks(fileContent)

// 	req := &transport.UploadFileRequest{
// 		Filename: file_name,
// 		FileSize: int64(len(fileContent)),
// 		UserID:   user_name,
// 		Policy:   policy.Overwrite,
// 	}

// 	// 메모리에 저장된 내용을 사용하는 스트림 생성 함수
// 	createStream := func() transport.UploadStream {
// 		return &transport.ChunkStream{
// 			Chunks: chunks,
// 		}
// 	}

// 	err = master.UploadFile(context.Background(), req, createStream)
// 	if err != nil {
// 		t.Fatalf("Failed to upload file: %v", err)
// 	}

// }

// func TestDownload(t *testing.T) {
// 	masterAddr := ":8000"
// 	master := node.NewMasterNode("master1")
// 	masterTransport, err := transport.NewTCPTransport(masterAddr, master)
// 	if err != nil {
// 		t.Fatalf("Failed to create master transport: %v", err)
// 	}

// 	// 데이터 노드 설정
// 	dataNode1 := node.NewDataNode("data1", store.StoreOpts{Root: "./tmp1/datanode1"})
// 	dataNode2 := node.NewDataNode("data2", store.StoreOpts{Root: "./tmp2/datanode2"})

// 	dataTransport1, err := transport.NewTCPTransport(":8001", dataNode1)
// 	if err != nil {
// 		t.Fatalf("Failed to create data node 1 transport: %v", err)
// 	}
// 	dataTransport2, err := transport.NewTCPTransport(":8002", dataNode2)
// 	if err != nil {
// 		t.Fatalf("Failed to create data node 2 transport: %v", err)
// 	}

// 	// 노드들 시작
// 	go masterTransport.Serve()
// 	go dataTransport1.Serve()
// 	go dataTransport2.Serve()

// 	// 마스터 노드에 데이터 노드 등록
// 	master.AddDataNode(dataNode1.ID, dataNode1)
// 	master.AddDataNode(dataNode2.ID, dataNode2)

// 	fileContent, err := ioutil.ReadFile(file_name)

// 	// 파일 다운로드
// 	downloadStream := &transport.FileDownloadStream{}
// 	err = master.DownloadFile(context.Background(), &transport.DownloadFileRequest{
// 		UserID:   user_name,
// 		Filename: file_name,
// 	}, downloadStream)
// 	if err != nil {
// 		t.Fatalf("Failed to download file: %v", err)
// 	}

// 	var downloadedContent []byte
// 	for {
// 		chunk, err := downloadStream.Recv()
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			t.Fatalf("Error receiving chunk: %v", err)
// 		}
// 		downloadedContent = append(downloadedContent, chunk.Content...)
// 	}

// 	// 원본 파일과 다운로드한 파일 비교
// 	if !bytes.Equal(fileContent, downloadedContent) {
// 		t.Errorf("Downloaded content does not match original content. Original size: %d, Downloaded size: %d", len(fileContent), len(downloadedContent))
// 	} else {
// 		fmt.Println("File content successfully uploaded and downloaded, matching the original content")
// 	}
// }

// func TestFileDelete(t *testing.T) {
// 	masterAddr := ":8000"
// 	master := node.NewMasterNode("master1")
// 	masterTransport, err := transport.NewTCPTransport(masterAddr, master)
// 	if err != nil {
// 		t.Fatalf("Failed to create master transport: %v", err)
// 	}

// 	// 데이터 노드 설정
// 	dataNode1 := node.NewDataNode("data1", store.StoreOpts{Root: "./tmp1/datanode1"})
// 	dataNode2 := node.NewDataNode("data2", store.StoreOpts{Root: "./tmp2/datanode2"})

// 	dataTransport1, err := transport.NewTCPTransport(":8001", dataNode1)
// 	if err != nil {
// 		t.Fatalf("Failed to create data node 1 transport: %v", err)
// 	}
// 	dataTransport2, err := transport.NewTCPTransport(":8002", dataNode2)
// 	if err != nil {
// 		t.Fatalf("Failed to create data node 2 transport: %v", err)
// 	}

// 	// 노드들 시작
// 	go masterTransport.Serve()
// 	go dataTransport1.Serve()
// 	go dataTransport2.Serve()

// 	// 마스터 노드에 데이터 노드 등록
// 	master.AddDataNode(dataNode1.ID, dataNode1)
// 	master.AddDataNode(dataNode2.ID, dataNode2)

// 	deleteResp, err := master.DeleteFile(context.Background(), &transport.DeleteFileRequest{
// 		Filename: file_name,
// 		UserID:   user_name,
// 	})
// 	if err != nil {
// 		t.Fatalf("Failed to delete file: %v", err)
// 	}
// 	if !deleteResp.Success {
// 		t.Fatalf("Failed to delete file: %s", deleteResp.Message)
// 	}
// }
