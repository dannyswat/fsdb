package fulltext

import (
	"os"
	"path/filepath"
	"testing"
)

// MockFileProvider for testing
type MockFileProvider struct {
	files map[string][]byte
	dirs  map[string]bool
}

func NewMockFileProvider() *MockFileProvider {
	return &MockFileProvider{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
}

func (m *MockFileProvider) CreateDirectory(path string) error {
	m.dirs[path] = true
	return nil
}

func (m *MockFileProvider) DirectoryExists(path string) (bool, error) {
	return m.dirs[path], nil
}

func (m *MockFileProvider) DeleteDirectory(path string) error {
	delete(m.dirs, path)
	// Remove all files in directory
	for key := range m.files {
		if filepath.Dir(key) == path {
			delete(m.files, key)
		}
	}
	return nil
}

func (m *MockFileProvider) FileExists(path, fileName string) (bool, error) {
	fullPath := filepath.Join(path, fileName)
	_, exists := m.files[fullPath]
	return exists, nil
}

func (m *MockFileProvider) ReadFile(path, fileName string) ([]byte, error) {
	fullPath := filepath.Join(path, fileName)
	data, exists := m.files[fullPath]
	if !exists {
		return nil, os.ErrNotExist
	}
	return data, nil
}

func (m *MockFileProvider) WriteFile(path, fileName string, data []byte) error {
	fullPath := filepath.Join(path, fileName)
	m.files[fullPath] = data
	return nil
}

func (m *MockFileProvider) DeleteFile(path, fileName string) error {
	fullPath := filepath.Join(path, fileName)
	delete(m.files, fullPath)
	return nil
}

type mockDirEntry struct {
	name  string
	isDir bool
}

func (m mockDirEntry) Name() string               { return m.name }
func (m mockDirEntry) IsDir() bool                { return m.isDir }
func (m mockDirEntry) Type() os.FileMode          { return 0 }
func (m mockDirEntry) Info() (os.FileInfo, error) { return nil, nil }

func (m *MockFileProvider) ReadDirectory(path string) ([]os.DirEntry, error) {
	var entries []os.DirEntry
	for file := range m.files {
		if filepath.Dir(file) == path {
			entries = append(entries, mockDirEntry{
				name:  filepath.Base(file),
				isDir: false,
			})
		}
	}
	return entries, nil
}

func TestInvertedIndex_AddAndSearch(t *testing.T) {
	mockProvider := NewMockFileProvider()
	idx, err := NewInvertedIndex("/test/index", 3, mockProvider)
	if err != nil {
		t.Fatalf("Failed to create inverted index: %v", err)
	}

	// Add documents
	err = idx.AddDocument("doc1", "Hello world")
	if err != nil {
		t.Fatalf("Failed to add document: %v", err)
	}

	err = idx.AddDocument("doc2", "Hello universe")
	if err != nil {
		t.Fatalf("Failed to add document: %v", err)
	}

	err = idx.AddDocument("doc3", "Goodbye world")
	if err != nil {
		t.Fatalf("Failed to add document: %v", err)
	}

	// Search for "hello"
	results, err := idx.Search("hello")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results for 'hello', got %d", len(results))
	}

	// Search for "world"
	results, err = idx.Search("world")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results for 'world', got %d", len(results))
	}

	// Search for "universe"
	results, err = idx.Search("universe")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result for 'universe', got %d", len(results))
	}

	if len(results) > 0 && results[0] != "doc2" {
		t.Errorf("Expected 'doc2' for 'universe', got %s", results[0])
	}
}

func TestInvertedIndex_ChineseText(t *testing.T) {
	mockProvider := NewMockFileProvider()
	idx, err := NewInvertedIndex("/test/index", 2, mockProvider) // Use bigrams for Chinese
	if err != nil {
		t.Fatalf("Failed to create inverted index: %v", err)
	}

	// Add Chinese documents
	err = idx.AddDocument("doc1", "你好世界")
	if err != nil {
		t.Fatalf("Failed to add Chinese document: %v", err)
	}

	err = idx.AddDocument("doc2", "世界和平")
	if err != nil {
		t.Fatalf("Failed to add Chinese document: %v", err)
	}

	// Search for "世界"
	results, err := idx.Search("世界")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results for '世界', got %d", len(results))
	}
}

func TestInvertedIndex_RemoveDocument(t *testing.T) {
	mockProvider := NewMockFileProvider()
	idx, err := NewInvertedIndex("/test/index", 3, mockProvider)
	if err != nil {
		t.Fatalf("Failed to create inverted index: %v", err)
	}

	// Add document
	err = idx.AddDocument("doc1", "Hello world")
	if err != nil {
		t.Fatalf("Failed to add document: %v", err)
	}

	// Verify it can be found
	results, err := idx.Search("hello")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result before removal, got %d", len(results))
	}

	// Remove document
	err = idx.RemoveDocument("doc1")
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
	}

	// Verify it's no longer found
	results, err = idx.Search("hello")
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results after removal, got %d", len(results))
	}
}
