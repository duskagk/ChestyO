package rest

import (
	"ChestyO/internal/enum"
	"ChestyO/internal/transport"
	"context"
	"encoding/json"
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

func (h *RESTHandler) HandleUpload(w http.ResponseWriter, r *http.Request) {
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
        Content  string `json:"content"`
        Policy   string `json:"policy"`
    }

    err := json.NewDecoder(r.Body).Decode(&requestBody)
    if err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    var policy enum.UploadPolicy
    switch requestBody.Policy {
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

    uploadRequest := &transport.UploadFileRequest{
        UserID:   requestBody.UserID,
        Filename: requestBody.Filename,
        FileSize: int64(len(requestBody.Content)),
        Policy:   policy,
        Content:  []byte(requestBody.Content),
    }

    err = h.masterService.UploadFile(opCtx,uploadRequest)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"message": "File uploaded successfully"})
}

func (h *RESTHandler) HandleDownload(w http.ResponseWriter, r *http.Request) {

    // fileContent, err := h.masterService.DownloadFile(r.Context(), downloadRequest)
    // if err != nil {
    //     http.Error(w, err.Error(), http.StatusInternalServerError)
    //     return
    // }
}

func (h *RESTHandler) HandleDelete(w http.ResponseWriter, r *http.Request) {

    // err := h.masterService.DeleteFile(r.Context(), deleteRequest)
    // if err != nil {
    //     http.Error(w, err.Error(), http.StatusInternalServerError)
    //     return
    // }
}