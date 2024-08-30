package rest

import (
	"ChestyO/internal/enum"
	"ChestyO/internal/transport"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// expiry 현재 시간에서 + expiry * second 으로 설정
// expiry 없을 시 default 값 7 days
var secretKey = []byte("a-very-secret-key-32-bytes-long!") // 실제 사용 시 안전하게 관리해야 함

type FileInfo struct {
    UserID    string    `json:"uid"`
    Filename  string    `json:"fn"`
    Expiration time.Time `json:"exp"`
}

var requestBody struct {
    UserID   string `json:"user_id"`
    Filename string `json:"filename"`
}

type RESTService interface {
    HandleUpload(w http.ResponseWriter, r *http.Request)
    HandleDownload(w http.ResponseWriter, r *http.Request)
    HandleDelete(w http.ResponseWriter, r *http.Request)
    HandleShareToken(w http.ResponseWriter, r *http.Request)
    HandleShareFileByToken(w http.ResponseWriter, r *http.Request)
    // 필요한 다른 핸들러들...
}

type RESTHandler struct {
    masterService transport.MasterFileService
}

func NewRESTHandler(masterService transport.MasterFileService) *RESTHandler {
    return &RESTHandler{
        masterService: masterService,
    }
}


//curl -X POST http://localhost:8080/upload -F "file=@/zxcv.txt" -F "user_id=user1" -F "policy=overwrite"
func (h *RESTHandler) HandleUpload(w http.ResponseWriter, r *http.Request) {
    opCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()

    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    file, header, err := r.FormFile("file")
    if err != nil {
        http.Error(w, "Error retrieving the file", http.StatusBadRequest)
        return
    }
    defer file.Close()


    fileContentType := header.Header.Get("Content-Type")

    if fileContentType == "" {
        http.Error(w, "No Mime type in file", http.StatusBadRequest)
        return
    }


    userID := r.FormValue("user_id")
    if userID == "" {
        http.Error(w, "User ID is required", http.StatusBadRequest)
        return
    }

    policyStr := r.FormValue("policy")
    var policy enum.UploadPolicy
    switch policyStr {
    case "overwrite":
        policy = enum.Overwrite
    case "version_control":
        policy = enum.VersionControl
    case "no_change":
        policy = enum.NoChange
    default:
        http.Error(w, "Invalid upload policy", http.StatusBadRequest)
        return
    }

    retentionPeriodStr := r.FormValue("retention_period_days")
    var retentionPeriodDays int
    if retentionPeriodStr != "" {
        retentionPeriodDays, err = strconv.Atoi(retentionPeriodStr)
        if err != nil {
            http.Error(w, "Invalid retention period", http.StatusBadRequest)
            return
        }
        // 유효성 검사: -1 (영구), 0 (기본값), 또는 양수
        if retentionPeriodDays < -1 {
            http.Error(w, "Retention period must be -1, 0, or a positive number", http.StatusBadRequest)
            return
        }
    } else {
        // 기본값 설정 (예: 0은 시스템 기본값 사용)
        retentionPeriodDays = 0
    }

    // 파일 내용 읽기
    content, err := io.ReadAll(file)
    if err != nil {
        http.Error(w, "Error reading the file", http.StatusInternalServerError)
        return
    }

    uploadRequest := &transport.UploadFileRequest{
        UserID:   userID,
        Filename: header.Filename,
        FileSize: header.Size,
        Policy:   policy,
        Content:  content,
        RetentionPeriodDays: retentionPeriodDays,
        FileContentType : fileContentType,
    }

    err = h.masterService.UploadFile(opCtx, uploadRequest)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
 
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"message": "File uploaded successfully"})
}

// 그냥 다운로드(만일을 위해)
func (h *RESTHandler) HandleDownload(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }


    opCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()

    userID := r.URL.Query().Get("user_id")
    filename := r.URL.Query().Get("filename")

    if userID == "" || filename == "" {
        http.Error(w, "Missing user_id or filename", http.StatusBadRequest)
        return
    }

    // exists, metadata, err := h.masterService.H(r.Context(), userID, filename)

    downloadRequest := &transport.DownloadFileRequest{
        Filename: requestBody.Filename,
        UserID: requestBody.UserID,
    }

    chunkChan,err := h.masterService.DownloadFile(opCtx,downloadRequest)
    if err !=nil{
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    if chunkChan!=nil{

        }else{
            w.WriteHeader(http.StatusOK)
        }
    
    // json.NewEncoder(w).Encode(map[string]string{"message": "File download successfully","file":string(file)})
}

func (h *RESTHandler) HandleDelete(w http.ResponseWriter, r *http.Request) {

    opCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()

    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    contentType := r.Header.Get("Content-Type")
    if contentType != "application/json" {
        http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
        return
    }

    var requestBody struct {
        UserID   string `json:"user_id"`
        Filename string `json:"filename"`
    }

    err := json.NewDecoder(r.Body).Decode(&requestBody)
    if err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    deleteRequest := &transport.DeleteFileRequest{
        Filename: requestBody.Filename,
        UserID: requestBody.UserID,
    }

    err = h.masterService.DeleteFile(opCtx, deleteRequest)
    if err !=nil{
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"message": "File delete successfully"})
}



func createToken(userID, filename string, expiry int) (string, error) {
    if expiry == 0 {
        expiry = 7 * 24 * 60 * 60 // 7 days in seconds
    }
    
    info := FileInfo{
        UserID:    userID,
        Filename:  filename,
        Expiration: time.Now().Add(time.Duration(expiry) * time.Second),
    }

    jsonData, err := json.Marshal(info)
    if err != nil {
        return "", err
    }

    block, err := aes.NewCipher(secretKey)
    if err != nil {
        return "", err
    }

    ciphertext := make([]byte, aes.BlockSize+len(jsonData))
    iv := ciphertext[:aes.BlockSize]
    if _, err := io.ReadFull(rand.Reader, iv); err != nil {
        return "", err
    }

    stream := cipher.NewCFBEncrypter(block, iv)
    stream.XORKeyStream(ciphertext[aes.BlockSize:], jsonData)

    return base64.URLEncoding.EncodeToString(ciphertext), nil
}

func validateToken(token string) (*FileInfo, error) {
    ciphertext, err := base64.URLEncoding.DecodeString(token)
    if err != nil {
        return nil, err
    }

    block, err := aes.NewCipher(secretKey)
    if err != nil {
        return nil, err
    }

    if len(ciphertext) < aes.BlockSize {
        return nil, fmt.Errorf("ciphertext too short")
    }
    iv := ciphertext[:aes.BlockSize]
    ciphertext = ciphertext[aes.BlockSize:]

    stream := cipher.NewCFBDecrypter(block, iv)
    stream.XORKeyStream(ciphertext, ciphertext)

    var info FileInfo
    if err := json.Unmarshal(ciphertext, &info); err != nil {
        return nil, err
    }

    if time.Now().After(info.Expiration) {
        return nil, fmt.Errorf("token expired")
    }

    return &info, nil
}

func (h *RESTHandler) HandleShareToken(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    var requestBody struct {
        UserID   string `json:"user_id"`
        Filename string `json:"filename"`
        Expiry   int    `json:"expiry"`
    }

    err := json.NewDecoder(r.Body).Decode(&requestBody)
    if err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    token, err := createToken(requestBody.UserID, requestBody.Filename, requestBody.Expiry)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    json.NewEncoder(w).Encode(map[string]string{"token": token})
}


func (h *RESTHandler) HandleShareFileByToken(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }


    filename := r.URL.Query().Get("filename")
    username := r.URL.Query().Get("user")
    metadataOnly := r.URL.Query().Get("metadata") == "true"

    exists, metadata, err := h.masterService.HasFile(r.Context(), username, filename)
    if !exists || err != nil {
        http.Error(w,fmt.Errorf("not Found File").Error() , http.StatusBadRequest)
        return
    }

    log.Printf("File Exist Check And metadata : %v %v", exists, metadata)

    if metadataOnly {
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(metadata)
        return
    }

    rangeHeader := r.Header.Get("Range")
    start, end := int64(0), metadata.FileSize-1

    if rangeHeader != "" {
        if strings.HasPrefix(rangeHeader, "bytes=") {
            rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
            rangeParts := strings.Split(rangeStr, "-")
            if len(rangeParts) == 2 {
                start, _ = strconv.ParseInt(rangeParts[0], 10, 64)
                if rangeParts[1] != "" {
                    end, _ = strconv.ParseInt(rangeParts[1], 10, 64)
                }else{
                    end = metadata.FileSize-1
                }
            }
        }
    }else{
        start = 0
        end = -1
    }

    ctx, cancel := context.WithCancel(r.Context())
    defer cancel()

    
    downloadRequest := &transport.DownloadFileRequest{
        Filename: filename,
        UserID:   username,
        Start:    start,
        End:      end,
    }

    chunks, err := h.masterService.DownloadFile(ctx, downloadRequest)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    totalLength := int64(0)
    for _, chunk := range chunks {
        totalLength += int64(len(chunk.Content))
    }

    w.Header().Set("Content-Type", metadata.ContentType)
    w.Header().Set("Accept-Ranges", "bytes")
    w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename*=UTF-8''%s", filename))
    w.Header().Set("Content-Length", fmt.Sprintf("%d", totalLength))


    if rangeHeader != "" {
        log.Printf("Content Range End : %v", downloadRequest.End)
        w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, metadata.FileSize))
        w.WriteHeader(http.StatusPartialContent)
    } else {
        w.WriteHeader(http.StatusOK)
    }

    for _, chunk := range chunks {
        select {
        case <-ctx.Done():
            log.Println("Client disconnected during response")
            return
        default:
            reader := bytes.NewReader(chunk.Content)
            bytesWritten, err := io.Copy(w, reader)
            // bytesWritten, err := w.Write(chunk.Content)
            if err != nil {
                log.Printf("Error writing to response: %v", err)
                return
            }
            log.Printf("Written by IO Copy %v byte", bytesWritten)
    
            if f, ok := w.(http.Flusher); ok {
                f.Flush()
            }
        }
    }

}



