// utils/chunk.go

package utils

import "ChestyO/internal/transport"



const ChunkSize = 50 // 1MB

func SplitFileIntoChunks(content []byte) []transport.FileChunk {
	var chunks []transport.FileChunk
	inx:=0
	for i := 0; i < len(content); i += ChunkSize {
		end := i + ChunkSize
		if end > len(content) {
			end = len(content)
		}
		chunks = append(chunks, transport.FileChunk{
			Content: content[i:end],
			Index: inx,
		})
		inx ++;
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
