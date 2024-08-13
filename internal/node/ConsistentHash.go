package node

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

type ConsistentHash struct {
    circle       map[uint32]string
    sortedHashes []uint32
    nodes        map[string]*DataNodeInfo
    virtualNodes int
    mu           sync.RWMutex
}

func NewConsistentHash(virtualNodes int) *ConsistentHash {
    return &ConsistentHash{
        circle:       make(map[uint32]string),
        sortedHashes: []uint32{},
        nodes:        make(map[string]*DataNodeInfo),
        virtualNodes: virtualNodes,
    }
}

func (ch *ConsistentHash) AddNode(node *DataNodeInfo) {
    ch.mu.Lock()
    defer ch.mu.Unlock()

    ch.nodes[node.ID] = node
    for i := 0; i < ch.virtualNodes; i++ {
        hash := ch.hashKey(fmt.Sprintf("%s:%d", node.ID, i))
        ch.circle[hash] = node.ID
        ch.sortedHashes = append(ch.sortedHashes, hash)
    }
    sort.Slice(ch.sortedHashes, func(i, j int) bool {
        return ch.sortedHashes[i] < ch.sortedHashes[j]
    })
}

func (ch *ConsistentHash) RemoveNode(nodeID string) {
    ch.mu.Lock()
    defer ch.mu.Unlock()

    delete(ch.nodes, nodeID)
    for i := 0; i < ch.virtualNodes; i++ {
        hash := ch.hashKey(fmt.Sprintf("%s:%d", nodeID, i))
        delete(ch.circle, hash)
    }
    ch.updateSortedHashes()
}

func (ch *ConsistentHash) GetNode(key string) (string, error) {
    ch.mu.RLock()
    defer ch.mu.RUnlock()

    if len(ch.nodes) == 0 {
        return "", errors.New("no available nodes")
    }

    hash := ch.hashKey(key)
    idx := sort.Search(len(ch.sortedHashes), func(i int) bool {
        return ch.sortedHashes[i] >= hash
    })
    if idx == len(ch.sortedHashes) {
        idx = 0
    }
    return ch.circle[ch.sortedHashes[idx]], nil
}

func (ch *ConsistentHash) hashKey(key string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(key))
    return h.Sum32()
}

func (ch *ConsistentHash) updateSortedHashes() {
    ch.sortedHashes = []uint32{}
    for hash := range ch.circle {
        ch.sortedHashes = append(ch.sortedHashes, hash)
    }
    sort.Slice(ch.sortedHashes, func(i, j int) bool {
        return ch.sortedHashes[i] < ch.sortedHashes[j]
    })
}