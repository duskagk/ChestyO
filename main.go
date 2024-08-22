package main

import (
	"ChestyO/internal/node"
	"ChestyO/internal/store"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
    nodeType := flag.String("type", "", "Type of node: 'master' or 'data'")
    nodeID := flag.String("id", "", "ID of the node")
    tcpAddr := flag.String("tcp", "", "TCP address to listen on")
    httpAddr := flag.String("http", "", "HTTP address to listen on (for master node)")
    masterAddr := flag.String("master", "", "Address of the master node (for data nodes)")
    numBuckets := flag.Int("buckets", 4, "Number of buckets for KVStore (default: 4)")
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
        masterNode := node.NewMasterNode(*nodeID, *tcpAddr, *httpAddr,*numBuckets)
        if masterNode == nil {
            log.Fatal("Failed to create MasterNode")
        }
        err = masterNode.Start(ctx)
    case "data":
        if *masterAddr == "" {
            log.Fatal("Master address is required for data node")
        }
        // err = node.RunDataNode(ctx, *nodeID, *tcpAddr, *masterAddr)
        
        dataNode := node.NewDataNode(*nodeID, store.StoreOpts{Root: fmt.Sprintf("./nfs/%s", *nodeID)}, *numBuckets)
        if dataNode ==nil{
            log.Fatalf("Failed to create Datanode")
        }
        dataNode.Start(ctx,*tcpAddr, *masterAddr)
    default:
        log.Fatal("Invalid node type. Use 'master' or 'data'")
    }

    if err != nil && err != context.Canceled {
        log.Printf("Node stopped with error: %v", err)
    } else {
        log.Println("Node stopped gracefully")
    }
}