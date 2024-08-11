package node

import (
	"sync"
	"time"
)

type FileMetadata struct {
    RetentionTime time.Time
    FileSize      int64
    ChunkNodes    []string
}

type UserMetadata struct {
    files map[string]FileMetadata
    mu    sync.RWMutex
}