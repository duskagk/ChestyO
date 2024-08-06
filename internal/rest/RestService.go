package rest

import (
	"ChestyO/internal/enum"
	"ChestyO/internal/transport"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type RESTService interface {
    HandleUpload(w http.ResponseWriter, r *http.Request)
    HandleDownload(w http.ResponseWriter, r *http.Request)
    HandleDelete(w http.ResponseWriter, r *http.Request)
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

    // 최대 파일 크기 설정 (예: 10MB)
    r.ParseMultipartForm(10 << 20)

    file, header, err := r.FormFile("file")
    if err != nil {
        http.Error(w, "Error retrieving the file", http.StatusBadRequest)
        return
    }
    defer file.Close()

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
    }

    err = h.masterService.UploadFile(opCtx, uploadRequest)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
 
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"message": "File uploaded successfully"})
}

func (h *RESTHandler) HandleDownload(w http.ResponseWriter, r *http.Request) {
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

    downloadRequest := &transport.DownloadFileRequest{
        Filename: requestBody.Filename,
        UserID: requestBody.UserID,
    }

    file,err := h.masterService.DownloadFile(opCtx,downloadRequest)
    if err !=nil{
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"message": "File download successfully","file":string(file)})
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