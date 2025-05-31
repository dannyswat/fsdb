package fsdb

import (
	"os"
	"path/filepath"
)

type FileProvider struct{}

func (fp *FileProvider) CreateDirectory(path string) error {
	return os.MkdirAll(path, 0755)
}

func (fp *FileProvider) DirectoryExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

func (fp *FileProvider) DeleteDirectory(path string) error {
	return os.RemoveAll(path)
}

func (fp *FileProvider) FileExists(path string, fileName string) (bool, error) {
	fullPath := filepath.Join(path, fileName)
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return !info.IsDir(), nil
}

func (fp *FileProvider) ReadFile(path string, fileName string) ([]byte, error) {
	fullPath := filepath.Join(path, fileName)
	return os.ReadFile(fullPath)
}

func (fp *FileProvider) WriteFile(path string, fileName string, data []byte) error {
	fullPath := filepath.Join(path, fileName)
	return os.WriteFile(fullPath, data, 0644)
}

func (fp *FileProvider) DeleteFile(path string, fileName string) error {
	fullPath := filepath.Join(path, fileName)
	return os.Remove(fullPath)
}

func (fp *FileProvider) ReadDirectory(path string) ([]os.DirEntry, error) {
	return os.ReadDir(path)
}
