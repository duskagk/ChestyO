package kvserver

import (
	"time"
)

type FileMetadata struct {
    RetentionTime time.Time
    FileSize      int64
    ChunkNodes    []string
}

type UserMetadata struct {
    Files map[string]FileMetadata
}
