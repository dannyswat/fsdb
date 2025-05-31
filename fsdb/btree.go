package fsdb

import (
	"errors"
	"math/rand"
	"time"
)

// BTree provides B+ tree operations using pluggable node storage.
type BTree struct {
	storage  BTreeNodeStorage
	rootID   string
	pageSize int
}

// NewBTree creates a new B+ tree with the given storage provider and page size.
func NewBTree(storage BTreeNodeStorage, rootID string, pageSize int) *BTree {
	return &BTree{
		storage:  storage,
		rootID:   rootID,
		pageSize: pageSize,
	}
}

// RootID returns the current root node ID.
func (bt *BTree) RootID() string {
	return bt.rootID
}

// Insert inserts a key-value pair into the B+ tree.
func (bt *BTree) Insert(key []any, value any) error {
	if bt.rootID == "" {
		// Create root as a new leaf node
		root := NewBTreeNode(generateNodeID(), LeafNode, bt.pageSize, "")
		root.Keys = append(root.Keys, key)
		root.Values = append(root.Values, value)
		if err := bt.storage.SaveNode(root); err != nil {
			return err
		}
		bt.rootID = root.ID
		return nil
	}
	root, err := bt.storage.LoadNode(bt.rootID)
	if err != nil {
		return err
	}
	return bt.insertRecursive(root, key, value)
}

// insertRecursive handles recursive insert and node splitting.
func (bt *BTree) insertRecursive(node *BTreeNode, key []any, value any) error {
	if node.IsLeaf() {
		// Insert in sorted order
		pos := 0
		for pos < len(node.Keys) && compareKeys(key, node.Keys[pos]) > 0 {
			pos++
		}
		node.Keys = append(node.Keys[:pos], append([][]any{key}, node.Keys[pos:]...)...)
		node.Values = append(node.Values[:pos], append([]any{value}, node.Values[pos:]...)...)
		node.IsDirty = true
		if !node.IsFull() {
			return bt.storage.SaveNode(node)
		}
		return bt.splitLeaf(node)
	}
	// Internal node: find child
	pos := 0
	for pos < len(node.Keys) && compareKeys(key, node.Keys[pos]) > 0 {
		pos++
	}
	childID := node.Values[pos].(string)
	child, err := bt.storage.LoadNode(childID)
	if err != nil {
		return err
	}
	err = bt.insertRecursive(child, key, value)
	if err != nil {
		return err
	}
	if child.IsFull() {
		// Child was split, handle promotion
		return bt.handleSplit(node, child)
	}
	return bt.storage.SaveNode(node)
}

// splitLeaf splits a full leaf node and promotes the middle key.
func (bt *BTree) splitLeaf(leaf *BTreeNode) error {
	mid := len(leaf.Keys) / 2
	right := NewBTreeNode(generateNodeID(), LeafNode, bt.pageSize, leaf.indexPath)
	right.Keys = append(right.Keys, leaf.Keys[mid:]...)
	right.Values = append(right.Values, leaf.Values[mid:]...)
	right.Next = leaf.Next
	right.Previous = leaf.ID
	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]
	leaf.Next = right.ID
	leaf.IsDirty = true
	if err := bt.storage.SaveNode(leaf); err != nil {
		return err
	}
	if err := bt.storage.SaveNode(right); err != nil {
		return err
	}
	if leaf.Parent == "" {
		// Create new root
		root := NewBTreeNode(generateNodeID(), InternalNode, bt.pageSize, leaf.indexPath)
		root.Keys = append(root.Keys, right.Keys[0])
		root.Values = append(root.Values, leaf.ID, right.ID)
		leaf.Parent = root.ID
		right.Parent = root.ID
		if err := bt.storage.SaveNode(root); err != nil {
			return err
		}
		bt.rootID = root.ID
		return nil
	}
	// Promote to parent
	parent, err := bt.storage.LoadNode(leaf.Parent)
	if err != nil {
		return err
	}
	return bt.insertInternalAfterSplit(parent, right.Keys[0], right.ID)
}

// insertInternalAfterSplit inserts a promoted key and right child ID into an internal node after a split.
func (bt *BTree) insertInternalAfterSplit(parent *BTreeNode, key []any, rightID string) error {
	pos := 0
	for pos < len(parent.Keys) && compareKeys(key, parent.Keys[pos]) > 0 {
		pos++
	}
	parent.Keys = append(parent.Keys[:pos], append([][]any{key}, parent.Keys[pos:]...)...)
	parent.Values = append(parent.Values[:pos+1], append([]any{rightID}, parent.Values[pos+1:]...)...)
	parent.IsDirty = true
	if !parent.IsFull() {
		return bt.storage.SaveNode(parent)
	}
	return bt.splitInternal(parent)
}

// splitInternal splits a full internal node and promotes the middle key.
func (bt *BTree) splitInternal(internal *BTreeNode) error {
	mid := len(internal.Keys) / 2
	right := NewBTreeNode(generateNodeID(), InternalNode, bt.pageSize, internal.indexPath)
	right.Keys = append(right.Keys, internal.Keys[mid+1:]...)
	right.Values = append(right.Values, internal.Values[mid+1:]...)
	promoteKey := internal.Keys[mid]
	internal.Keys = internal.Keys[:mid]
	internal.Values = internal.Values[:mid+1]
	if err := bt.storage.SaveNode(internal); err != nil {
		return err
	}
	if err := bt.storage.SaveNode(right); err != nil {
		return err
	}
	if internal.Parent == "" {
		// New root
		root := NewBTreeNode(generateNodeID(), InternalNode, bt.pageSize, internal.indexPath)
		root.Keys = append(root.Keys, promoteKey)
		root.Values = append(root.Values, internal.ID, right.ID)
		internal.Parent = root.ID
		right.Parent = root.ID
		if err := bt.storage.SaveNode(root); err != nil {
			return err
		}
		bt.rootID = root.ID
		return nil
	}
	parent, err := bt.storage.LoadNode(internal.Parent)
	if err != nil {
		return err
	}
	return bt.insertInternalAfterSplit(parent, promoteKey, right.ID)
}

// handleSplit is called after a child node is split.
func (bt *BTree) handleSplit(parent, child *BTreeNode) error {
	// No-op: split logic already handled in splitLeaf/splitInternal
	return nil
}

// Search returns all values matching the given key (or all if key is nil).
func (bt *BTree) Search(key []any) ([]any, error) {
	if bt.rootID == "" {
		return nil, nil
	}
	node, err := bt.storage.LoadNode(bt.rootID)
	if err != nil {
		return nil, err
	}
	for !node.IsLeaf() {
		pos := 0
		for pos < len(node.Keys) && (key == nil || compareKeys(key, node.Keys[pos]) > 0) {
			pos++
		}
		childID := node.Values[pos].(string)
		node, err = bt.storage.LoadNode(childID)
		if err != nil {
			return nil, err
		}
	}
	// At leaf
	results := []any{}
	for i, k := range node.Keys {
		if key == nil || compareKeys(key, k) == 0 {
			results = append(results, node.Values[i])
		}
	}
	return results, nil
}

// Delete removes a key-value pair from the B+ tree (not fully implemented).
func (bt *BTree) Delete(key []any, value any) error {
	return errors.New("Delete not implemented")
}

// Utility: compareKeys compares two composite keys.
func compareKeys(a, b []any) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		switch va := a[i].(type) {
		case int:
			vb := b[i].(int)
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
		case string:
			vb := b[i].(string)
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			// Add more types as needed
		}
	}
	return len(a) - len(b)
}

// Utility: generateNodeID returns a unique node ID (placeholder).
func generateNodeID() string {
	// In production, use a UUID or atomic counter
	return RandString(16)
}

// RandString generates a random string of n characters (for node IDs).
func RandString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[seededRand.Intn(len(letters))]
	}
	return string(b)
}

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
