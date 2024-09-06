// utils/chunk.go

package utils

import "ChestyO/internal/transport"



const ChunkSize = 1024 * 1024 * 4 // 1MB

func SplitFileIntoChunks(content []byte) []*transport.FileChunk {
	chunks := make([]*transport.FileChunk, 0, len(content)/ChunkSize+1)
	for i := 0; i < len(content); i += ChunkSize {
		end := i + ChunkSize
		if end > len(content) {
			end = len(content)
		}
		chunks = append(chunks, &transport.FileChunk{
			Content: content[i:end],
			Index:   i / ChunkSize,
		})
	}
	return chunks
}

func Contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}
