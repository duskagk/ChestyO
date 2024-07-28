// test/upload_test.go

package test

import (
	policy "ChestyO/internal/enum"
	"ChestyO/internal/node"
	"ChestyO/internal/store"
	"ChestyO/internal/transport"
	"ChestyO/internal/utils"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
)

var file_name = "zxcv.txt"
var user_name = "TestUser"

func TestFileUpload(t *testing.T) {
	// 마스터 노드 설정
	masterAddr := ":8000"
	master := node.NewMasterNode("master1")
	masterTransport, err := transport.NewTCPTransport(masterAddr, master)
	if err != nil {
		t.Fatalf("Failed to create master transport: %v", err)
	}

	// 데이터 노드 설정
	dataNode1 := node.NewDataNode("data1", store.StoreOpts{Root: "./tmp1/datanode1"})
	dataNode2 := node.NewDataNode("data2", store.StoreOpts{Root: "./tmp2/datanode2"})

	dataTransport1, err := transport.NewTCPTransport(":8001", dataNode1)
	if err != nil {
		t.Fatalf("Failed to create data node 1 transport: %v", err)
	}
	dataTransport2, err := transport.NewTCPTransport(":8002", dataNode2)
	if err != nil {
		t.Fatalf("Failed to create data node 2 transport: %v", err)
	}

	// 노드들 시작
	go masterTransport.Serve()
	go dataTransport1.Serve()
	go dataTransport2.Serve()

	// 마스터 노드에 데이터 노드 등록
	master.AddDataNode(dataNode1.ID, dataNode1)
	master.AddDataNode(dataNode2.ID, dataNode2)

	// 테스트 파일 내용을 메모리에 저장
	fileContent, err := ioutil.ReadFile(file_name)
	if err != nil {
		t.Fatalf("Failed to read test file: %v", err)
	}

	chunks := utils.SplitFileIntoChunks(fileContent)

	req := &transport.UploadFileRequest{
		Filename: file_name,
		FileSize: int64(len(fileContent)),
		UserID:   user_name,
		Policy:   policy.Overwrite,
	}

	// 메모리에 저장된 내용을 사용하는 스트림 생성 함수
	createStream := func() transport.UploadStream {
		return &transport.ChunkStream{
			Chunks: chunks,
		}
	}

	err = master.UploadFile(context.Background(), req, createStream)
	if err != nil {
		t.Fatalf("Failed to upload file: %v", err)
	}

}

func TestDownload(t *testing.T) {
	masterAddr := ":8000"
	master := node.NewMasterNode("master1")
	masterTransport, err := transport.NewTCPTransport(masterAddr, master)
	if err != nil {
		t.Fatalf("Failed to create master transport: %v", err)
	}

	// 데이터 노드 설정
	dataNode1 := node.NewDataNode("data1", store.StoreOpts{Root: "./tmp1/datanode1"})
	dataNode2 := node.NewDataNode("data2", store.StoreOpts{Root: "./tmp2/datanode2"})

	dataTransport1, err := transport.NewTCPTransport(":8001", dataNode1)
	if err != nil {
		t.Fatalf("Failed to create data node 1 transport: %v", err)
	}
	dataTransport2, err := transport.NewTCPTransport(":8002", dataNode2)
	if err != nil {
		t.Fatalf("Failed to create data node 2 transport: %v", err)
	}

	// 노드들 시작
	go masterTransport.Serve()
	go dataTransport1.Serve()
	go dataTransport2.Serve()

	// 마스터 노드에 데이터 노드 등록
	master.AddDataNode(dataNode1.ID, dataNode1)
	master.AddDataNode(dataNode2.ID, dataNode2)

	fileContent, err := ioutil.ReadFile(file_name)

	// 파일 다운로드
	downloadStream := &transport.FileDownloadStream{}
	err = master.DownloadFile(context.Background(), &transport.DownloadFileRequest{
		UserID:   user_name,
		Filename: file_name,
	}, downloadStream)
	if err != nil {
		t.Fatalf("Failed to download file: %v", err)
	}

	var downloadedContent []byte
	for {
		chunk, err := downloadStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error receiving chunk: %v", err)
		}
		downloadedContent = append(downloadedContent, chunk.Content...)
	}

	// 원본 파일과 다운로드한 파일 비교
	if !bytes.Equal(fileContent, downloadedContent) {
		t.Errorf("Downloaded content does not match original content. Original size: %d, Downloaded size: %d", len(fileContent), len(downloadedContent))
	} else {
		fmt.Println("File content successfully uploaded and downloaded, matching the original content")
	}
}

func TestFileDelete(t *testing.T) {
	masterAddr := ":8000"
	master := node.NewMasterNode("master1")
	masterTransport, err := transport.NewTCPTransport(masterAddr, master)
	if err != nil {
		t.Fatalf("Failed to create master transport: %v", err)
	}

	// 데이터 노드 설정
	dataNode1 := node.NewDataNode("data1", store.StoreOpts{Root: "./tmp1/datanode1"})
	dataNode2 := node.NewDataNode("data2", store.StoreOpts{Root: "./tmp2/datanode2"})

	dataTransport1, err := transport.NewTCPTransport(":8001", dataNode1)
	if err != nil {
		t.Fatalf("Failed to create data node 1 transport: %v", err)
	}
	dataTransport2, err := transport.NewTCPTransport(":8002", dataNode2)
	if err != nil {
		t.Fatalf("Failed to create data node 2 transport: %v", err)
	}

	// 노드들 시작
	go masterTransport.Serve()
	go dataTransport1.Serve()
	go dataTransport2.Serve()

	// 마스터 노드에 데이터 노드 등록
	master.AddDataNode(dataNode1.ID, dataNode1)
	master.AddDataNode(dataNode2.ID, dataNode2)

	deleteResp, err := master.DeleteFile(context.Background(), &transport.DeleteFileRequest{
		Filename: file_name,
		UserID:   user_name,
	})
	if err != nil {
		t.Fatalf("Failed to delete file: %v", err)
	}
	if !deleteResp.Success {
		t.Fatalf("Failed to delete file: %s", deleteResp.Message)
	}
}
