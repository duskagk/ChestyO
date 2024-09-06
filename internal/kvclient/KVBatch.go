package kvclient

import (
	"sync"
)

// KVPair represents a key-value pair with operation type
type KVPair struct {
    Type  string      `json:"type"`
    Key   string      `json:"key"`
    Value interface{} `json:"value"`
}

// KVBatch manages a batch of key-value pairs
type KVBatch struct {
    pairs []KVPair
    mu    sync.Mutex
}

// NewKVBatch creates a new KVBatch
func NewKVBatch() *KVBatch {
    return &KVBatch{
        pairs: make([]KVPair, 0),
    }
}

// Add appends a new key-value pair to the batch
func (b *KVBatch) Add(type_ string, key string, value interface{}) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.pairs = append(b.pairs, KVPair{Type: type_, Key: key, Value: value})
}

// AddPair appends a KVPair to the batch
func (b *KVBatch) AddPair(pair KVPair) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.pairs = append(b.pairs, pair)
}

// AddMultiple appends multiple key-value pairs to the batch
func (b *KVBatch) AddMultiple(pairs ...KVPair) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.pairs = append(b.pairs, pairs...)
}

// Clear empties the batch
func (b *KVBatch) Clear() {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.pairs = b.pairs[:0]
}

// Size returns the number of pairs in the batch
func (b *KVBatch) Size() int {
    b.mu.Lock()
    defer b.mu.Unlock()
    return len(b.pairs)
}

// GetPairs returns a copy of the current pairs in the batch
func (b *KVBatch) GetPairs() []KVPair {
    b.mu.Lock()
    defer b.mu.Unlock()
    pairs := make([]KVPair, len(b.pairs))
    copy(pairs, b.pairs)
    return pairs
}