package main

import (
	"ChestyO/internal/node"
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
    nodeType := flag.String("type", "", "Type of node: 'master' or 'data'")
    tcpAddr := flag.String("tcp", "", "TCP address to listen on")
    httpAddr := flag.String("http", "", "HTTP address to listen on (for master node)")
    masterAddr := flag.String("master", "", "Address of the master node (for data nodes)")
    storePath := flag.String("path","","")
    flag.Parse()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 시그널 처리
    go func() {
        c := make(chan os.Signal, 1)
        signal.Notify(c, os.Interrupt, syscall.SIGTERM)
        <-c
        cancel()
    }()

    var err error
    switch *nodeType {
    case "master":
        if *httpAddr == "" {
            log.Fatal("HTTP address is required for master node")
        }
        log.Printf("TCP addr : %v, HttpADDR : %v", *tcpAddr,*httpAddr)
        masterNode := node.NewMasterNode(*tcpAddr, *httpAddr)
        if masterNode == nil {
            log.Fatal("Failed to create MasterNode")
        }
        err = masterNode.Start(ctx)
    case "data":    
        dataNode := node.NewDataNode(*storePath,*tcpAddr,*masterAddr)
        if dataNode ==nil{
            log.Fatalf("Failed to create Datanode")
        }
        err = dataNode.Start(ctx)
    default:
        log.Fatal("Invalid node type. Use 'master' or 'data'")
    }

    if err != nil && err != context.Canceled {
        log.Printf("Node stopped with error: %v", err)
    } else {
        log.Println("Node stopped gracefully")
    }
}