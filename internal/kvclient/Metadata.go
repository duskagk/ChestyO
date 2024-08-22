package kvclient

import (
	"time"
)

type FileMetadata struct {
    RetentionTime time.Time `json:"retentionTime"`
    FileSize      int64     `json:"fileSize"`
    ChunkNodes    []string  `json:"chunkNodes"`
}

type UserMetadata struct {
    Files map[string]FileMetadata
}
