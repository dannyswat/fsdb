package fsdb

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// BTreeNodeStorage abstracts node persistence for different file providers.
type BTreeNodeStorage interface {
	SaveNode(node *BTreeNode) error
	LoadNode(nodeID string) (*BTreeNode, error)
}

// FileBTreeNodeStorage implements BTreeNodeStorage using the local filesystem.
type FileBTreeNodeStorage struct {
	IndexPath string
}

func (fs *FileBTreeNodeStorage) SaveNode(node *BTreeNode) error {
	if !node.IsDirty {
		return nil
	}
	if fs.IndexPath == "" || node.ID == "" {
		return os.ErrInvalid
	}
	filePath := filepath.Join(fs.IndexPath, node.ID+".json")
	data, err := json.MarshalIndent(node, "", "  ")
	if err != nil {
		return err
	}
	err = os.WriteFile(filePath, data, 0644)
	if err == nil {
		node.IsDirty = false
	}
	return err
}

func (fs *FileBTreeNodeStorage) LoadNode(nodeID string) (*BTreeNode, error) {
	filePath := filepath.Join(fs.IndexPath, nodeID+".json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var node BTreeNode
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, err
	}
	node.indexPath = fs.IndexPath
	node.IsDirty = false
	return &node, nil
}
