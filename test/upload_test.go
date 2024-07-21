// test/upload_test.go

package test

import (
	"ChestyO/internal/node"
	"ChestyO/internal/store"
	"ChestyO/internal/transport"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
)

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
	fileContent, err := ioutil.ReadFile("zxcv.txt")
	if err != nil {
		t.Fatalf("Failed to read test file: %v", err)
	}

	req := &transport.UploadFileRequest{
		Filename: "zxcv.txt",
		FileSize: int64(len(fileContent)),
		UserID:   "TestUser",
	}

	// 메모리에 저장된 내용을 사용하는 스트림 생성 함수
	createStream := func() transport.UploadStream {
		return &mockUploadStream{content: fileContent}
	}

	err = master.UploadFile(context.Background(), req, createStream)
	if err != nil {
		t.Fatalf("Failed to upload file: %v", err)
	}

	// 파일 다운로드
	downloadStream := &transport.FileDownloadStream{}
	err = master.DownloadFile(context.Background(), &transport.DownloadFileRequest{
		UserId:   req.UserID,
		Filename: req.Filename,
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

// mockUploadStream은 테스트를 위한 UploadStream의 mock 구현입니다.
type mockUploadStream struct {
	content []byte
	index   int
}

func (m *mockUploadStream) Send(chunk *transport.FileChunk) error {
	// 이 메서드는 실제로 사용되지 않습니다.
	return nil
}

func (m *mockUploadStream) Recv() (*transport.FileChunk, error) {
	if m.index >= len(m.content) {
		return nil, io.EOF
	}

	chunkSize := 1024 // 1KB 청크 크기
	if m.index+chunkSize > len(m.content) {
		chunkSize = len(m.content) - m.index
	}

	chunk := &transport.FileChunk{
		Content: m.content[m.index : m.index+chunkSize],
	}
	m.index += chunkSize
	return chunk, nil
}

func (m *mockUploadStream) CloseAndRecv() (*transport.UploadFileResponse, error) {
	return &transport.UploadFileResponse{
		Success: true,
		Message: "File uploaded successfully",
	}, nil
}
