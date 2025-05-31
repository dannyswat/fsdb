package fsdb

import "os"

// IFileProvider abstracts file and directory operations for the database.
type IFileProvider interface {
	CreateDirectory(path string) error
	DirectoryExists(path string) (bool, error)
	DeleteDirectory(path string) error
	FileExists(path string, fileName string) (bool, error)
	ReadFile(path string, fileName string) ([]byte, error)
	WriteFile(path string, fileName string, data []byte) error
	DeleteFile(path string, fileName string) error
	ReadDirectory(path string) ([]os.DirEntry, error)
}
