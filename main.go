package main

import (
	"ChestyO/internal/node"
	"flag"
	"log"
)

func main() {
	nodeType := flag.String("type", "", "Type of node: 'master' or 'data'")
	nodeID := flag.String("id", "", "ID of the node")
	listenAddr := flag.String("addr", "", "Address to listen on")
	masterAddr := flag.String("master", "", "Address of the master node (for data nodes)")
	flag.Parse()

	switch *nodeType {
	case "master":
		err := node.RunMasterNode(*nodeID, *listenAddr)
		if err != nil {
			log.Fatal(err)
		}

	case "data":
		err := node.RunDataNode(*nodeID, *listenAddr, *masterAddr)
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("Invalid node type. Use 'master' or 'data'")
	}
}
