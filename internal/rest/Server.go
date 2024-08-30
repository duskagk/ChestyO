package rest

import (
	"ChestyO/internal/transport"
	"context"
	"log"
	"net/http"
	"strings"
	"time"
)

var allowedIPs = []string{
	"172.27.192.1", // 예시 IP 주소, 실제 IP로 변경
	"172.27.112.1",
}

type RestServer struct{
	server		*http.Server
	handler     RESTService
}


func NewServer(masterNode transport.MasterFileService, addr string) *RestServer {
    return &RestServer{
        server: &http.Server{
            Addr: addr,
            ReadTimeout:  10 * time.Minute,
            WriteTimeout: 10 * time.Minute,
        },
        handler: NewRESTHandler(masterNode),
    }
}



func ipFilterMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientIP := strings.Split(r.RemoteAddr, ":")[0]
        log.Printf("Current ClientIP : %v", clientIP)
		allowed := false
		for _, ip := range allowedIPs {
			if clientIP == ip {
				allowed = true
				break
			}
		}
		if !allowed {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func (s *RestServer) Serve(ctx context.Context) error {
    mux := http.NewServeMux()
    
    // 라우트 설정
	mux.HandleFunc("/upload", s.handler.HandleUpload)
	mux.HandleFunc("/download", ipFilterMiddleware(s.handler.HandleDownload))
	mux.HandleFunc("/delete", ipFilterMiddleware(s.handler.HandleDelete))
	mux.HandleFunc("/sharetoken", ipFilterMiddleware(s.handler.HandleShareToken))
	mux.HandleFunc("/sharefile", s.handler.HandleShareFileByToken) // 공개된 핸들러
    s.server.Handler = mux

    errChan := make(chan error, 1)
    go func() {
        log.Printf("Starting REST server on %s", s.server.Addr)
        errChan <- s.server.ListenAndServe()
    }()

    select {
    case <-ctx.Done():
        return s.server.Shutdown(context.Background())
    case err := <-errChan:
        return err
    }
}

func (s *RestServer) Stop() error {
    return s.server.Shutdown(context.Background())
}