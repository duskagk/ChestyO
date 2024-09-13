package rest

import (
	"ChestyO/internal/enum"
	"ChestyO/internal/transport"
	"ChestyO/internal/utils"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

type GinServer struct {
	router  		*gin.Engine
	masterNode 		transport.MasterFileService
	addr 			string
}

type contentTypeInfo struct {
    ContentType string
}
const (
    metadataDir = "/tmp/metadata"
    uploadsDir  = "/tmp/uploads"
)
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


    router.MaxMultipartMemory = 25 << 20

    funcMap := template.FuncMap{
        "sub": func(a, b int) int {
            return a - b
        },
        "add": func(a, b int) int {
            return a + b
        },
    }


    config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	config.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
	config.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Authorization"}
	router.Use(cors.New(config))
    
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
	s.router.GET("/", s.handleDashboard)
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


func (s *GinServer) handleUpload(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

    c.Request = c.Request.WithContext(ctx)

	filename := c.PostForm("filename")
	userID := c.PostForm("bucket")
	policyStr := c.PostForm("policy")
	retentionDay := c.PostForm("retention_days")

	// Check if chunkNumber and totalChunks are present
	chunkNumberStr := c.PostForm("chunkNumber")
	totalChunksStr := c.PostForm("totalChunks")


	if chunkNumberStr != "" && totalChunksStr != "" {
		// Handle chunked upload
		chunkNumber, _ := strconv.Atoi(chunkNumberStr)
		totalChunks, _ := strconv.Atoi(totalChunksStr)
		s.handleChunkedUpload(c,chunkNumber, totalChunks, filename, userID, policyStr, retentionDay)
	} else {
		// Handle regular (non-chunked) upload
		s.handleRegularUpload(c, filename, userID, policyStr, retentionDay)
	}
}

func (s *GinServer) handleChunkedUpload(c *gin.Context,  chunkNumber, totalChunks int, filename, userID, policyStr, retentionPeriodStr string) {
	ctx := c.Request.Context()
    
    select {
    case <- ctx.Done():
        c.JSON(http.StatusRequestTimeout, gin.H{"error": "Request timed out"})
        return
    default:
        var fileContentType string
        if chunkNumber == 0 {
            fileContentType = c.PostForm("content_type")
            if fileContentType == "" {
                fileContentType = "application/octet-stream"
                log.Printf("No content type provided, using default: %v", fileContentType)
            } else {
                log.Printf("Got content type from chunk 0: %v", fileContentType)
            }
            s.saveContentType(userID, filename, fileContentType)
        }
    
        // 파일 청크 가져오기
        file, err := c.FormFile("file")
        if err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Error retrieving the file chunk"})
            return
        }
    
        // 청크 데이터 읽기
        chunkData, err := file.Open()
        if err != nil {
            
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Error reading chunk data"})
            return
        }
        defer chunkData.Close()
    
        // 청크 저장 (임시 파일 또는 메모리에 저장)
        err = s.saveChunk(userID, filename, chunkNumber, chunkData)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Error saving chunk"})
            return
        }
    
        // 마지막 청크인 경우 전체 파일 처리
        if chunkNumber == totalChunks-1 {

            go func(){
                ctx, cancel := context.WithTimeout(context.Background(), 5* time.Minute)
                defer cancel()
                err := s.finalizeChunkedUpload(ctx,userID, filename, totalChunks, policyStr, retentionPeriodStr)
                if err != nil{
                    log.Printf("Error finalizing chunked upload: %v", err)
                }
            }()
            c.JSON(http.StatusOK, gin.H{"message": "File uploaded started, processing"})
        } else {
            c.JSON(http.StatusOK, gin.H{"message": "Chunk uploaded successfully"})
        }
    }
}

func (s *GinServer) handleRegularUpload(c *gin.Context, filename, userID, policyStr, retentionPeriodStr string) {
	ctx := c.Request.Context()
    
    select {
    case <- ctx.Done():
        c.JSON(http.StatusRequestTimeout, gin.H{"error": "Request timed out"})
        return
    default:
        file, err := c.FormFile("file")
        if err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Error retrieving the file"})
            return
        }
    
        fileContent, err := file.Open()
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Error opening file"})
            return
        }
        defer fileContent.Close()
    
        content, err := io.ReadAll(fileContent)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Error reading file content"})
            return
        }
    
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
    
        retentionPeriodDays, _ := strconv.Atoi(retentionPeriodStr)
    
        contentType := c.PostForm("content_type")
        if contentType == "" {
            contentType = file.Header.Get("Content-Type")
        }
    
        uploadRequest := &transport.UploadFileRequest{
            UserID:              userID,
            Filename:            filename,
            FileSize:            file.Size,
            Policy:              policy,
            Content:             content,
            RetentionPeriodDays: retentionPeriodDays,
            FileContentType:     contentType,
        }
    
        err = s.masterNode.UploadFile(ctx, uploadRequest)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
    
        c.JSON(http.StatusOK, gin.H{"message": "File uploaded successfully"})
    }
}

func (s *GinServer) finalizeChunkedUpload(ctx context.Context, userID, filename string, totalChunks int, policyStr, retentionPeriodStr string) error {
	var policy enum.UploadPolicy
	switch policyStr {
	case "overwrite":
		policy = enum.Overwrite
	case "version_control":
		policy = enum.VersionControl
	case "no_change":
		policy = enum.NoChange
	default:
		return fmt.Errorf("invalid upload policy")
	}

	retentionPeriodDays, _ := strconv.Atoi(retentionPeriodStr)

	fileContent, fileSize, err := s.combineChunks(userID, filename, totalChunks)
	if err != nil {
		return fmt.Errorf("error combining chunks: %w", err)
	}

	content_type, err := s.getContentType(userID, filename)
	if err != nil {
		log.Printf("Get content type is error: %v", err)
	}

	uploadRequest := &transport.UploadFileRequest{
		UserID:              userID,
		Filename:            filename,
		FileSize:            fileSize,
		Policy:              policy,
		Content:             fileContent,
		RetentionPeriodDays: retentionPeriodDays,
		FileContentType:     content_type,
	}

	err = s.masterNode.UploadFile(ctx, uploadRequest)
	if err != nil {
		return err
	}

	s.cleanupChunks(userID, filename)
	return nil
}

// func (s *GinServer) handleUpload(c *gin.Context){
// 	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
// 	defer cancel()

// 	// 청크 정보 가져오기
//     chunkNumber, _ := strconv.Atoi(c.PostForm("chunkNumber"))
//     totalChunks, _ := strconv.Atoi(c.PostForm("totalChunks"))
//     filename := c.PostForm("filename")
//     userID := c.PostForm("bucket")
//     policyStr := c.PostForm("policy")
//     retentionPeriodStr := c.PostForm("retention_period_days")


//     var fileContentType string
//     if chunkNumber == 0 {
//         fileContentType = c.PostForm("content_type")
//         log.Printf("Get Content type from chunk 0 : %v", fileContentType)
//         // Content-Type을 서버에 저장 (예: 메모리 또는 데이터베이스)
//         s.saveContentType(userID, filename, fileContentType)
//     }

// 	// 파일 청크 가져오기
// 	file, err := c.FormFile("file")
// 	if err != nil {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Error retrieving the file chunk"})
// 		return
// 	}

// 	// 청크 데이터 읽기
// 	chunkData, err := file.Open()
// 	if err != nil {
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error reading chunk data"})
// 		return
// 	}
// 	defer chunkData.Close()

// 	// 청크 저장 (임시 파일 또는 메모리에 저장)
// 	err = s.saveChunk(userID, filename, chunkNumber, chunkData)
// 	if err != nil {
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error saving chunk"})
// 		return
// 	}

// 	// 마지막 청크인 경우 전체 파일 처리
// 	if chunkNumber == totalChunks-1 {
// 		var policy enum.UploadPolicy
// 		switch policyStr {
// 		case "overwrite":
// 			policy = enum.Overwrite
// 		case "version_control":
// 			policy = enum.VersionControl
// 		case "no_change":
// 			policy = enum.NoChange
// 		default:
// 			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid upload policy"})
// 			return
// 		}

// 		retentionPeriodDays, _ := strconv.Atoi(retentionPeriodStr)

// 		// 전체 파일 조합 및 업로드
// 		fileContent, fileSize, err := s.combineChunks(userID, filename, totalChunks)
// 		if err != nil {
// 			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error combining chunks"})
// 			return
// 		}

//         content_type , err := s.getContentType(userID,filename)
//         if err !=nil{
//             log.Printf("Get content type is error : %v", err)
//         }
// 		uploadRequest := &transport.UploadFileRequest{
// 			UserID:              userID,
// 			Filename:            filename,
// 			FileSize:            fileSize,
// 			Policy:              policy,
// 			Content:             fileContent,
// 			RetentionPeriodDays: retentionPeriodDays,
// 			FileContentType:     content_type,
// 		}

// 		err = s.masterNode.UploadFile(ctx, uploadRequest)
// 		if err != nil {
// 			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
// 			return
// 		}

// 		// 임시 청크 파일 삭제
// 		s.cleanupChunks(userID, filename)

// 		c.JSON(http.StatusOK, gin.H{"message": "File uploaded successfully"})
// 	} else {
// 		c.JSON(http.StatusOK, gin.H{"message": "Chunk uploaded successfully"})
// 	}
// }

func (s *GinServer) handleShareFileByToken(c *gin.Context) {
    filename := c.Query("file")
    username := c.Query("bucket")

    exists, metadata, err := s.masterNode.HasFile(c.Request.Context(), username, filename)
    if !exists || err != nil {
        c.String(http.StatusBadRequest, "File not found")
        return
    }

    log.Printf("File Exist Check And metadata : %v %v", exists, metadata)

    rangeHeader := c.GetHeader("Range")
    start, end := int64(0), metadata.FileSize-1
    var contentLength int64

    if rangeHeader != "" {
        if strings.HasPrefix(rangeHeader, "bytes=") {
            rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
            rangeParts := strings.Split(rangeStr, "-")
            if len(rangeParts) > 0 {
                start, _ = strconv.ParseInt(rangeParts[0], 10, 64)
                if len(rangeParts) > 1 && rangeParts[1] != "" {
                    end, _ = strconv.ParseInt(rangeParts[1], 10, 64)
                }
            }
        }
        contentLength = end - start + 1

        maxResponseSize := int64(utils.ChunkSize)
        if contentLength > maxResponseSize {
            end = start + maxResponseSize - 1
            contentLength = maxResponseSize
        }
    } else {
        start = 0
        end = metadata.FileSize - 1
        contentLength = metadata.FileSize
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
        log.Printf("Error downloading file: %v", err)
        c.String(http.StatusInternalServerError, "Error downloading file")
        return
    }

    if len(chunks) == 0 {
        log.Printf("No chunks returned for file: %s", filename)
        c.String(http.StatusInternalServerError, "No data available")
        return
    }

    c.Header("Content-Type", metadata.ContentType)
    c.Header("Accept-Ranges", "bytes")
    c.Header("Content-Disposition", fmt.Sprintf("attachment; filename*=UTF-8''%s", filename))
    c.Header("Content-Length", fmt.Sprintf("%d", contentLength))

    if rangeHeader != "" {
        log.Printf("Content Range: bytes %d-%d/%d", start, end, metadata.FileSize)
        c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, metadata.FileSize))
        c.Status(http.StatusPartialContent)
    } else {
        c.Status(http.StatusOK)
    }

    bytesWritten := int64(0)
    for _, chunk := range chunks {
        select {
        case <-ctx.Done():
            log.Println("Client disconnected during response")
            return
        default:
            reader := bytes.NewReader(chunk.Content)
            written, err := io.Copy(c.Writer, reader)
            if err != nil {
                log.Printf("Error writing chunk to response: %v", err)
                return
            }
            bytesWritten += written
            log.Printf("Written chunk: Index %d, Length %d", chunk.Index, written)
    
            c.Writer.Flush()

            if bytesWritten >= contentLength {
                log.Printf("Completed writing file: %s, Total bytes written: %d", filename, bytesWritten)
                return
            }
        }
    }

    if bytesWritten < contentLength {
        log.Printf("Warning: Incomplete file transfer. Expected %d bytes, wrote %d bytes", contentLength, bytesWritten)
    }
}

// func (s *GinServer) handleShareFileByToken(c *gin.Context) {
//     filename := c.Query("file")
//     username := c.Query("bucket")

//     exists, metadata, err := s.masterNode.HasFile(c.Request.Context(), username, filename)
//     if !exists || err != nil {
//         c.String(http.StatusBadRequest, "File not found")
//         return
//     }

//     log.Printf("File Exist Check And metadata : %v %v", exists, metadata)

//     rangeHeader := c.GetHeader("Range")
//     start, end := int64(0), metadata.FileSize-1
//     var contentLength int64

//     if rangeHeader != "" {
//         if strings.HasPrefix(rangeHeader, "bytes=") {
//             rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
//             rangeParts := strings.Split(rangeStr, "-")
//             if len(rangeParts) > 0 {
//                 start, _ = strconv.ParseInt(rangeParts[0], 10, 64)
//                 if len(rangeParts) > 1 && rangeParts[1] != "" {
//                     end, _ = strconv.ParseInt(rangeParts[1], 10, 64)
//                 }
//             }
//         }
//         contentLength = end - start + 1

//         maxResponseSize := int64(utils.ChunkSize)
//         if contentLength > maxResponseSize {
//             end = start + maxResponseSize - 1
//             contentLength = maxResponseSize
//         }
//     } else {
//         start = 0
//         end = metadata.FileSize - 1
//         contentLength = metadata.FileSize
//     }



//     ctx, cancel := context.WithCancel(c.Request.Context())
//     defer cancel()

//     downloadRequest := &transport.DownloadFileRequest{
//         Filename: filename,
//         UserID:   username,
//         Start:    start,
//         End:      end,
//     }

//     chunks, err := s.masterNode.DownloadFile(ctx, downloadRequest)
//     if err != nil {
//         c.String(http.StatusInternalServerError, err.Error())
//         return
//     }


//     c.Header("Content-Type", metadata.ContentType)
//     c.Header("Accept-Ranges", "bytes")
//     c.Header("Content-Disposition", fmt.Sprintf("attachment; filename*=UTF-8''%s", filename))
//     c.Header("Content-Length", fmt.Sprintf("%d", contentLength))

//     if rangeHeader != "" {
//         log.Printf("Content Range End : %v", downloadRequest.End)
//         c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, metadata.FileSize))
//         c.Status(http.StatusPartialContent)
//     } else {
//         c.Status(http.StatusOK)
//     }

//     bytesWritten := int64(0)
//     for _, chunk := range chunks {
//         select {
//         case <-ctx.Done():
//             log.Println("Client disconnected during response")
//             return
//         default:
//             reader := bytes.NewReader(chunk.Content)
//             written, err := io.Copy(c.Writer, reader)
//             if err != nil {
//                 log.Printf("Error writing to response: %v", err)
//                 return
//             }
//             bytesWritten += written
//             log.Printf("Written by IO Copy %v byte", written)
    
//             c.Writer.Flush()

//             if bytesWritten >= contentLength {
//                 return
//             }
//         }
//     }
// }

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




func (s *GinServer) saveChunk(userID, filename string, chunkNumber int, chunkData io.Reader) error {
	tempDir := filepath.Join(uploadsDir, fmt.Sprintf("%s_%s", userID, filename))
	if err := os.MkdirAll(tempDir, os.ModePerm); err != nil {
		return err
	}

	tempFile := filepath.Join(tempDir, fmt.Sprintf("chunk_%d", chunkNumber))
	out, err := os.Create(tempFile)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, chunkData)
	return err
}

func (s *GinServer) combineChunks(userID, filename string, totalChunks int) ([]byte, int64, error) {
	tempDir := filepath.Join(uploadsDir, fmt.Sprintf("%s_%s", userID, filename))
	var combinedFile bytes.Buffer
	var totalSize int64

	for i := 0; i < totalChunks; i++ {
		chunkPath := filepath.Join(tempDir, fmt.Sprintf("chunk_%d", i))
		chunkData, err := os.ReadFile(chunkPath)
		if err != nil {
			return nil, 0, err
		}
		combinedFile.Write(chunkData)
		totalSize += int64(len(chunkData))
	}

	return combinedFile.Bytes(), totalSize, nil
}


func (s *GinServer) cleanupChunks(userID, filename string) {
	tempDir := filepath.Join(uploadsDir, fmt.Sprintf("%s_%s", userID, filename))
	os.RemoveAll(tempDir)
    s.removeContentType(userID, filename)
}


func (s *GinServer) saveContentType(userID, filename, contentType string) error {
    if err := os.MkdirAll(metadataDir, 0755); err != nil {
        return fmt.Errorf("error creating metadata directory: %v", err)
    }

    filePath := filepath.Join(metadataDir, fmt.Sprintf("%s_%s.gob", userID, filename))
    file, err := os.Create(filePath)
    if err != nil {
        return fmt.Errorf("error creating content type file: %v", err)
    }
    defer file.Close()

    encoder := gob.NewEncoder(file)
    info := contentTypeInfo{ContentType: contentType}
    if err := encoder.Encode(info); err != nil {
        return fmt.Errorf("error encoding content type: %v", err)
    }

    return nil
}

func (s *GinServer) getContentType(userID, filename string) (string, error) {
    filePath := filepath.Join(metadataDir, fmt.Sprintf("%s_%s.gob", userID, filename))
    file, err := os.Open(filePath)
    if err != nil {
        return "application/octet-stream", fmt.Errorf("error opening content type file: %v", err)
    }
    defer file.Close()

    var info contentTypeInfo
    decoder := gob.NewDecoder(file)
    if err := decoder.Decode(&info); err != nil {
        s.removeContentType(userID, filename)
        return "application/octet-stream", fmt.Errorf("error decoding content type: %v", err)
    }

    return info.ContentType, nil
}

func (s *GinServer) removeContentType(userID, filename string) error {
    filePath := filepath.Join(metadataDir, fmt.Sprintf("%s_%s.gob", userID, filename))
    if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
        return fmt.Errorf("error removing content type file: %v", err)
    }
    return nil
}
