package fsdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// NodeType represents the type of B+ tree node
type NodeType string

const (
	LeafNode     NodeType = "leaf"
	InternalNode NodeType = "internal"
)

// BTreeNode represents a node in the B+ tree
type BTreeNode struct {
	ID       string   `json:"id"`
	Type     NodeType `json:"type"`
	Keys     []any    `json:"keys"`     // Index keys
	Values   []any    `json:"values"`   // For leaf nodes: actual data rows, for internal nodes: child node IDs
	Parent   string   `json:"parent"`   // Parent node ID
	Next     string   `json:"next"`     // Next leaf node ID (for leaf nodes only)
	Previous string   `json:"previous"` // Previous leaf node ID (for leaf nodes only)
	PageSize int      `json:"page_size"`
	IsDirty  bool     `json:"-"` // Not serialized, used to track if node needs to be saved
}

// NewLeafNode creates a new leaf node
func NewLeafNode(id string, pageSize int) *BTreeNode {
	return &BTreeNode{
		ID:       id,
		Type:     LeafNode,
		Keys:     make([]any, 0),
		Values:   make([]any, 0),
		PageSize: pageSize,
		IsDirty:  true,
	}
}

// NewInternalNode creates a new internal node
func NewInternalNode(id string, pageSize int) *BTreeNode {
	return &BTreeNode{
		ID:       id,
		Type:     InternalNode,
		Keys:     make([]any, 0),
		Values:   make([]any, 0),
		PageSize: pageSize,
		IsDirty:  true,
	}
}

// IsLeaf returns true if this is a leaf node
func (n *BTreeNode) IsLeaf() bool {
	return n.Type == LeafNode
}

// IsFull returns true if the node is full
func (n *BTreeNode) IsFull() bool {
	return len(n.Keys) >= n.PageSize
}

// SaveToFile saves the node to a file
func (n *BTreeNode) SaveToFile(basePath string) error {
	if !n.IsDirty {
		return nil
	}

	filePath := filepath.Join(basePath, fmt.Sprintf("%s.json", n.ID))

	data, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal node %s: %w", n.ID, err)
	}

	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write node %s to file: %w", n.ID, err)
	}

	n.IsDirty = false
	return nil
}

// LoadFromFile loads a node from a file
func LoadNodeFromFile(basePath, nodeID string) (*BTreeNode, error) {
	filePath := filepath.Join(basePath, fmt.Sprintf("%s.json", nodeID))

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read node %s from file: %w", nodeID, err)
	}

	var node BTreeNode
	err = json.Unmarshal(data, &node)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal node %s: %w", nodeID, err)
	}

	node.IsDirty = false
	return &node, nil
}

// DeleteFile removes the node file from disk
func (n *BTreeNode) DeleteFile(basePath string) error {
	filePath := filepath.Join(basePath, fmt.Sprintf("%s.json", n.ID))
	return os.Remove(filePath)
}
