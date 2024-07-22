package store

// internal/store/Store.go

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const defaultRootFolderName = "globenetwork"

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	return PathKey{
		Pathname: hashStr, // 첫 5자리만 디렉토리로 사용
		Filename: hashStr, // 나머지는 파일명으로 사용
	}
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	Pathname string
	Filename string
}

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.Pathname, "/")
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.Pathname, p.Filename)
}

type StoreOpts struct {
	// Root is the folder name of the root, containing all of the folders/files of the system.
	Root string
	// ID of the owner of the storage, which will be used to store all files at that location
	// so we can sync all the files if needed.
	PathTransformFunc PathTransformFunc
}

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		Pathname: key,
		Filename: key,
	}
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = CASPathTransformFunc
	}
	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}
	return &Store{
		StoreOpts: opts,
	}
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

func (s *Store) Has(id string, key string) bool {
	pathKey := s.PathTransformFunc(key)
	dirPath := filepath.Join(s.Root, id, pathKey.Pathname)

	_, err := os.Stat(dirPath)
	return !errors.Is(err, os.ErrNotExist)

}

func (s *Store) Delete(id string, key string) error {
	pathKey := s.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.Filename)
	}()

	firstPathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FirstPathName())

	return os.RemoveAll(firstPathNameWithRoot)
}

func (s *Store) Write(id string, originalFilename string, chunkName string, r io.Reader) (int64, error) {
	return s.writeStream(id, originalFilename, chunkName, r)
}

// func (s *Store) WriteDecrypt(encKey []byte,id string, key string, r io.Reader)(int64, error){
// 	f,err := s.openFileForWriting(id,key)
// 	if err != nil{
// 		return 0, err
// 	}
// 	defer f.Close()
// 	n, _ := secureio.CopyDecrypt(encKey,r,f)
// 	return int64(n),nil
// }

func (s *Store) openFileForWriting(id string, originalFilename string, chunkName string) (*os.File, error) {
	pathKey := s.PathTransformFunc(originalFilename)
	dirPath := filepath.Join(s.Root, id, pathKey.Pathname)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %v", dirPath, err)
	}

	fullPath := filepath.Join(dirPath, chunkName)
	return os.Create(fullPath)
}

func (s *Store) writeStream(id string, originalFilename string, chunkName string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, originalFilename, chunkName)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(f, r)
}

func (s *Store) Read(id string, originalFilename string, chunkName string) (int64, io.Reader, error) {
	return s.readStream(id, originalFilename, chunkName)
}

func (s *Store) readStream(id string, originalFilename string, chunkName string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(originalFilename)
	fullPath := filepath.Join(s.Root, id, pathKey.Pathname, chunkName)

	file, err := os.Open(fullPath)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to open file %s: %v", fullPath, err)
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return 0, nil, fmt.Errorf("failed to get file info for %s: %v", fullPath, err)
	}

	return fi.Size(), file, nil
}
