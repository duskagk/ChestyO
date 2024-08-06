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
    nodeID := flag.String("id", "", "ID of the node")
    listenAddr := flag.String("addr", "", "Address to listen on")
    masterAddr := flag.String("master", "", "Address of the master node (for data nodes)")
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
        err = node.RunMasterNode(ctx, *nodeID, *listenAddr)
    case "data":
        err = node.RunDataNode(ctx, *nodeID, *listenAddr, *masterAddr)
    default:
        log.Fatal("Invalid node type. Use 'master' or 'data'")
    }

    if err != nil && err != context.Canceled {
        log.Printf("Node stopped with error: %v", err)
    } else {
        log.Println("Node stopped gracefully")
    }
}