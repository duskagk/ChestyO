package transport

import (
	"encoding/gob"
	"net"
)

type MessageType int

const (
    MessageType_REGISTER MessageType = iota
    MessageType_UPLOAD
    MessageType_DOWNLOAD
    MessageType_DELETE
    MessageType_LIST
)

type Message struct {
    Type             MessageType
    RegisterMessage  *RegisterMessage
    UploadRequest    *UploadFileRequest
    DownloadRequest  *DownloadFileRequest
    DeleteRequest    *DeleteFileRequest
    ListRequest      *ListFilesRequest
}


func SendMessage(conn net.Conn, msg *Message) error {
    encoder := gob.NewEncoder(conn)
    return encoder.Encode(msg)
}

func ReceiveMessage(conn net.Conn) (*Message, error) {
    decoder := gob.NewDecoder(conn)
    msg := &Message{}
    err := decoder.Decode(msg)
    return msg, err
}