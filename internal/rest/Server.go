package rest

import (
	"ChestyO/internal/transport"
	"context"
	"embed"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type RestServer struct {
	server  		*http.Server
	handler 		RESTService
	router  		*gin.Engine
	masterNode 		transport.MasterFileService
}

//go:embed static/*
var staticFS embed.FS

func NewServer(masterNode transport.MasterFileService, addr string) *RestServer {
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
	
	server := &RestServer{
		server: &http.Server{
			Addr:         addr,
			Handler:      router,
			ReadTimeout:  10 * time.Minute,
			WriteTimeout: 10 * time.Minute,
		},
		handler: NewRESTHandler(masterNode),
		router:  router,
		masterNode: masterNode,
	}

	server.setupRoutes()
	return server
}

func (s *RestServer) setupRoutes() {
	s.router.POST("/upload", gin.WrapF(s.handler.HandleUpload))
	s.router.GET("/download", gin.WrapF(s.handler.HandleDownload))
	s.router.DELETE("/delete", gin.WrapF(s.handler.HandleDelete))
	s.router.POST("/sharetoken", gin.WrapF(s.handler.HandleShareToken))
	s.router.GET("/sharefile", gin.WrapF(s.handler.HandleShareFileByToken))

	// 대시보드 라우트 추가
	s.router.GET("/dashboard", s.handleDashboard)

	// 정적 파일 제공 (HTML, CSS, JS 등)
	s.router.StaticFS("/static", http.FS(staticFS))
}

func (s *RestServer) handleDashboard(c *gin.Context) {
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

func (s *RestServer) Serve(ctx context.Context) error {
	log.Printf("Starting REST server on %s", s.server.Addr)

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Listen: %s\n", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exiting")
	return nil
}

func (s *RestServer) Stop() error {
	return s.server.Shutdown(context.Background())
}