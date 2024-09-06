package rest

import (
	"ChestyO/internal/enum"
	"ChestyO/internal/transport"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type GinServer struct {
	router  		*gin.Engine
	masterNode 		transport.MasterFileService
	addr 			string
}



type FileInfo struct {
        UserID    string    `json:"uid"`
        Filename  string    `json:"fn"`
        Expiration time.Time `json:"exp"`
    }


//go:embed static/*
var staticFS embed.FS
var secretKey = []byte("a-very-secret-key-32-bytes-long!")
func NewServer(masterNode transport.MasterFileService, addr string) *GinServer {
	router := gin.Default()

    funcMap := template.FuncMap{
        "sub": func(a, b int) int {
            return a - b
        },
        "add": func(a, b int) int {
            return a + b
        },
    }


	tmpl := template.Must(template.New("").Funcs(funcMap).ParseFS(staticFS, "static/*"))
	router.SetHTMLTemplate(tmpl)
	
	server := &GinServer{
		addr : addr,
		router:  router,
		masterNode: masterNode,
	}

	server.setupRoutes()
	return server
}

func (s *GinServer) setupRoutes() {
	s.router.POST("/upload", s.handleUpload)
	// s.router.GET("/download", gin.WrapF(s.handler.HandleDownload))
	s.router.DELETE("/delete", s.handleDelete)
	s.router.POST("/sharetoken", s.handleShareToken)
	s.router.GET("/sharefile", s.handleShareFileByToken)

	// 대시보드 라우트 추가
	s.router.GET("/dashboard", s.handleDashboard)
	s.router.GET("/filelist", s.handleFileList)

	// 정적 파일 제공 (HTML, CSS, JS 등)
	s.router.StaticFS("/static", http.FS(staticFS))
    s.router.GET("/fileinfo", s.handleFileInfo)
}

func (s *GinServer) handleDashboard(c *gin.Context) {
	// 여기서 대시보드에 필요한 데이터를 준비합니다.
	// 예: 총 파일 수, 저장 공간 사용량 등
    offset, err := strconv.Atoi(c.DefaultQuery("offset", "0"))
    if err != nil {
        offset = 0
    }
    
    limit, err := strconv.Atoi(c.DefaultQuery("limit", "100"))
    if err != nil {
        limit = 100
    }

	buckets, err := s.masterNode.GetBuckets(c.Request.Context(), limit, offset)
	log.Printf("error when bucket get : %v",buckets)
    if err != nil {
        c.HTML(http.StatusInternalServerError, "error.html", gin.H{"error": err.Error()})
        return
    }

    data := gin.H{
        "title":   "File Storage Dashboard",
        "buckets": buckets,
        "offset":  offset,
        "limit":   limit,
        "nextOffset": offset + len(buckets),
    }

	// HTML 파일을 제공합니다.
	c.HTML(http.StatusOK, "dashboard.html",data)
}

func (s *GinServer) handleFileList(c *gin.Context) {
    bucket := c.Query("bucket")
    if bucket == "" {
        c.HTML(http.StatusBadRequest, "error.html", gin.H{"error": "Bucket name is required"})
        return
    }

    offset, err := strconv.Atoi(c.DefaultQuery("offset", "0"))
    if err != nil {
        offset = 0
    }
    
    limit, err := strconv.Atoi(c.DefaultQuery("limit", "100"))
    if err != nil {
        limit = 100
    }

    if limit > 1000 {
        limit = 1000
    }

    req := &transport.FileListRequest{
        Bucket: bucket,
        Limit:  limit,
        Offset: offset,
    }

    resp, err := s.masterNode.GetFileList(c.Request.Context(), req)
    if err != nil {
        c.HTML(http.StatusInternalServerError, "error.html", gin.H{"error": err.Error()})
        return
    }

    data := gin.H{
        "title":      "File List - " + bucket,
        "bucket":     bucket,
        "files":      resp.Files,
        "offset":     offset,
        "limit":      limit,
        "nextOffset": offset + len(resp.Files),
    }

    c.HTML(http.StatusOK, "filelist.html", data)
}

func (s *GinServer) handleFileInfo(c *gin.Context) {
	bucket := c.Query("bucket")
	filename := c.Query("filename")

	if bucket == "" || filename == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Bucket and filename are required"})
		return
	}

	metadata, err := s.masterNode.GetFileMetadata(c.Request.Context(), bucket, filename)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	response := gin.H{
		"filename":      filename,
		"bucket":        bucket,
		"fileSize":      metadata.FileSize,
		"contentType":   metadata.ContentType,
		"retentionTime": metadata.RetentionTime,
	}

	c.JSON(http.StatusOK, response)
}

func (s *GinServer) handleUpload(c *gin.Context){
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
    defer cancel()
    c.Request = c.Request.WithContext(ctx)

	log.Printf("Start upload handler : %v", c.Request)
    // 파일 가져오기

    reader, err := c.Request.MultipartReader()
    if err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Error creating multipart reader"})
        return
    }

    var filename string
    var fileSize int64
    var fileContentType string
    var userID string
    var policyStr string
    var retentionPeriodStr string
    var content []byte

    // 각 파트 처리
    for {
        part, err := reader.NextPart()
        if err == io.EOF {
            break
        }
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Error reading multipart data"})
            return
        }

        switch part.FormName() {
        case "file":
            filename = part.FileName()
            // 파일 내용을 읽습니다. 주의: 대용량 파일의 경우 메모리 문제가 발생할 수 있습니다.
            content, err = io.ReadAll(part)
            if err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": "Error reading file content"})
                return
            }
            fileSize = int64(len(content))
        case "bucket":
            userIDBytes, _ := io.ReadAll(part)
            userID = string(userIDBytes)
        case "policy":
            policyBytes, _ := io.ReadAll(part)
            policyStr = string(policyBytes)
        case "retention_period_days":
            retentionPeriodBytes, _ := io.ReadAll(part)
            retentionPeriodStr = string(retentionPeriodBytes)
        case "content_type":
            contentTypeBytes, _ := io.ReadAll(part)
            fileContentType = string(contentTypeBytes)
        }
    }
	log.Printf("Get File : %v", filename)
	if filename == "" || userID == "" || policyStr == "" || fileContentType == "" {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Missing required fields"})
        return
    }



    // 업로드 정책 확인

    var policy enum.UploadPolicy
    switch policyStr {
    case "overwrite":
        policy = enum.Overwrite
    case "version_control":
        policy = enum.VersionControl
    case "no_change":
        policy = enum.NoChange
    default:
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid upload policy"})
        return
    }


    var retentionPeriodDays int
    if retentionPeriodStr != "" {
        retentionPeriodDays, err = strconv.Atoi(retentionPeriodStr)
        if err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid retention period"})
            return
        }
        if retentionPeriodDays < -1 {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Retention period must be -1, 0, or a positive number"})
            return
        }
    } else {
        retentionPeriodDays = 0
    }

    // 파일 내용 읽기

    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Error reading the file"})
        return
    }

    // 업로드 요청 생성
    uploadRequest := &transport.UploadFileRequest{
        UserID:              userID,
        Filename:            filename,
        FileSize:            fileSize,
        Policy:              policy,
        Content:             content,
        RetentionPeriodDays: retentionPeriodDays,
        FileContentType:     fileContentType,
    }

    // 파일 업로드 실행
    err = s.masterNode.UploadFile(ctx, uploadRequest)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    // 성공 응답
    c.JSON(http.StatusOK, gin.H{"message": "File uploaded successfully"})
}


func (s *GinServer) handleShareFileByToken(c *gin.Context) {
    filename := c.Query("file")
    username := c.Query("bucket")
    metadataOnly := c.Query("metadata") == "true"

    exists, metadata, err := s.masterNode.HasFile(c.Request.Context(), username, filename)
    if !exists || err != nil {
        c.String(http.StatusBadRequest, "File not found")
        return
    }

    log.Printf("File Exist Check And metadata : %v %v", exists, metadata)

    if metadataOnly {
        c.Header("Content-Type", "application/json")
        c.JSON(http.StatusOK, metadata)
        return
    }

    rangeHeader := c.GetHeader("Range")
    start, end := int64(0), metadata.FileSize-1

    if rangeHeader != "" {
        if strings.HasPrefix(rangeHeader, "bytes=") {
            rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
            rangeParts := strings.Split(rangeStr, "-")
            if len(rangeParts) == 2 {
                start, _ = strconv.ParseInt(rangeParts[0], 10, 64)
                if rangeParts[1] != "" {
                    end, _ = strconv.ParseInt(rangeParts[1], 10, 64)
                } else {
                    end = metadata.FileSize-1
                }
            }
        }
    } else {
        start = 0
        end = -1
    }

    ctx, cancel := context.WithCancel(c.Request.Context())
    defer cancel()

    downloadRequest := &transport.DownloadFileRequest{
        Filename: filename,
        UserID:   username,
        Start:    start,
        End:      end,
    }

    chunks, err := s.masterNode.DownloadFile(ctx, downloadRequest)
    if err != nil {
        c.String(http.StatusInternalServerError, err.Error())
        return
    }

    totalLength := int64(0)
    for _, chunk := range chunks {
        totalLength += int64(len(chunk.Content))
    }

    c.Header("Content-Type", metadata.ContentType)
    c.Header("Accept-Ranges", "bytes")
    c.Header("Content-Disposition", fmt.Sprintf("attachment; filename*=UTF-8''%s", filename))
    c.Header("Content-Length", fmt.Sprintf("%d", totalLength))

    if rangeHeader != "" {
        log.Printf("Content Range End : %v", downloadRequest.End)
        c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, metadata.FileSize))
        c.Status(http.StatusPartialContent)
    } else {
        c.Status(http.StatusOK)
    }

    for _, chunk := range chunks {
        select {
        case <-ctx.Done():
            log.Println("Client disconnected during response")
            return
        default:
            reader := bytes.NewReader(chunk.Content)
            bytesWritten, err := io.Copy(c.Writer, reader)
            if err != nil {
                log.Printf("Error writing to response: %v", err)
                return
            }
            log.Printf("Written by IO Copy %v byte", bytesWritten)
    
            c.Writer.Flush()
        }
    }
}

func (s *GinServer) handleDelete(c *gin.Context){
    var requestBody struct {
        Bucket   string `json:"bucket"`
        Filename string `json:"filename"`
    }

    if err := c.ShouldBindJSON(&requestBody); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
        return
    }

    deleteRequest := &transport.DeleteFileRequest{
        Filename: requestBody.Filename,
        UserID:   requestBody.Bucket,
    }

    err := s.masterNode.DeleteFile(c.Request.Context(), deleteRequest)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    c.JSON(http.StatusOK, gin.H{"message": "File delete successfully"})
}

func (s *GinServer) handleShareToken(c *gin.Context) {
    var requestBody struct {
        UserID   string `json:"user_id"`
        Filename string `json:"filename"`
        Expiry   int    `json:"expiry"`
    }

    if err := c.ShouldBindJSON(&requestBody); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
        return
    }

    token, err := createToken(requestBody.UserID, requestBody.Filename, requestBody.Expiry)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    c.JSON(http.StatusOK, gin.H{"token": token})
}

func (s *GinServer) Serve(ctx context.Context) error {
	go func(){
		if err := s.router.Run(s.addr); err != nil{
			log.Printf("Error starting server: %v\n", err)
		}
	}()

    <-ctx.Done()
    log.Println("Shutting down server...")
	
	// log.Println("Server exiting")
	return nil
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
