package kvserver

import (
	"fmt"
	"sync"
)


type Bucket struct {
	globalID string
    data map[string]interface{}
    mu   sync.RWMutex
}

func NewBucket(nodeID string, localIndex int) *Bucket {
    return &Bucket{
		globalID: fmt.Sprintf("%s-%d", nodeID, localIndex),
        data: make(map[string]interface{}),
    }
}

func (b *Bucket) Set(key string, value interface{}) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.data[key] = value
}

func (b *Bucket) Get(key string) (interface{}, bool) {
    b.mu.RLock()
    defer b.mu.RUnlock()
    value, ok := b.data[key]
    return value, ok
}

func (b *Bucket) Delete(key string) {
    b.mu.Lock()
    defer b.mu.Unlock()
    delete(b.data, key)
}