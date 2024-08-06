package rest

import (
	"ChestyO/internal/transport"
	"context"
	"log"
	"net/http"
)



type RestServer struct{
	server		*http.Server
	handler     RESTService
}


func NewServer(masterNode transport.MasterFileService, addr string) *RestServer {
    return &RestServer{
        server: &http.Server{
            Addr: addr,
        },
        handler: NewRESTHandler(masterNode),
    }
}

func (s *RestServer) Serve(ctx context.Context) error {
    mux := http.NewServeMux()
    
    // 라우트 설정
    mux.HandleFunc("/upload", s.handler.HandleUpload)
    mux.HandleFunc("/download", s.handler.HandleDownload)
    mux.HandleFunc("/delete", s.handler.HandleDelete)
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