package fsdb

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// BTree provides B+ tree operations using pluggable node storage.
type BTree struct {
	storage     BTreeNodeStorage
	rootID      string
	pageSize    int
	isUniqueKey bool // true if this is a clustered index
}

// NewBTree creates a new B+ tree with the given storage provider and page size.
func NewBTree(storage BTreeNodeStorage, rootID string, pageSize int, isUniqueKey bool) *BTree {
	return &BTree{
		storage:     storage,
		rootID:      rootID,
		pageSize:    pageSize,
		isUniqueKey: isUniqueKey,
	}
}

// RootID returns the current root node ID.
func (bt *BTree) RootID() string {
	return bt.rootID
}

// Insert inserts a key-value pair into the B+ tree.
func (bt *BTree) Insert(key []any, value any) error {
	log.Printf("Insert: key=%#v, value=%#v", key, value)
	if bt.isUniqueKey {
		// Only check for duplicates if the tree is not empty
		if bt.rootID != "" {
			results, err := bt.Search(key)
			if err != nil {
				return err
			}
			if len(results) > 0 {
				return fmt.Errorf("duplicate key not allowed in clustered index: %#v", key)
			}
		}
	}
	if bt.rootID == "" {
		log.Printf("Insert: creating new root leaf node")
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
		log.Printf("Insert: failed to load root node %s: %v", bt.rootID, err)
		return err
	}
	return bt.insertRecursive(root, key, value)
}

// insertRecursive handles recursive insert and node splitting.
func (bt *BTree) insertRecursive(node *BTreeNode, key []any, value any) error {
	log.Printf("insertRecursive: nodeID=%s, isLeaf=%v, key=%#v", node.ID, node.IsLeaf(), key)
	if node.IsLeaf() {
		// Insert in sorted order
		pos := 0
		for pos < len(node.Keys) && compareKeys(key, node.Keys[pos]) > 0 {
			pos++
		}
		// Check for duplicate key if clustered (unique key)
		if bt.isUniqueKey {
			// Check all keys for duplicate (not just neighbors)
			for i := 0; i < len(node.Keys); i++ {
				if compareKeys(key, node.Keys[i]) == 0 {
					return fmt.Errorf("duplicate key not allowed in clustered index: %#v", key)
				}
			}
		}
		// For non-clustered, allow duplicates: insert after all existing duplicates
		for !bt.isUniqueKey && pos < len(node.Keys) && compareKeys(key, node.Keys[pos]) == 0 {
			pos++
		}
		log.Printf("insertRecursive: inserting at pos=%d, key=%#v, value=%#v", pos, key, value)
		node.Keys = append(node.Keys[:pos], append([][]any{key}, node.Keys[pos:]...)...)
		node.Values = append(node.Values[:pos], append([]any{value}, node.Values[pos:]...)...)
		node.IsDirty = true
		log.Printf("insertRecursive: leaf node %s after insert, keys=%#v, values=%#v", node.ID, node.Keys, node.Values)
		if !node.IsFull() {
			return bt.storage.SaveNode(node)
		}
		log.Printf("insertRecursive: leaf node %s is full, splitting", node.ID)
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
	log.Printf("insertRecursive: internal node %s after insert, keys=%#v", node.ID, node.Keys)
	return bt.storage.SaveNode(node)
}

// splitLeaf splits a full leaf node and promotes the middle key.
func (bt *BTree) splitLeaf(leaf *BTreeNode) error {
	log.Printf("splitLeaf: splitting leaf node %s, keys=%#v", leaf.ID, leaf.Keys)
	mid := len(leaf.Keys) / 2
	right := NewBTreeNode(generateNodeID(), LeafNode, bt.pageSize, leaf.indexPath)
	right.Keys = append(right.Keys, leaf.Keys[mid:]...)
	right.Values = append(right.Values, leaf.Values[mid:]...)
	right.Next = leaf.Next
	right.Previous = leaf.ID
	// Fix next/previous pointers of the right neighbor if it exists
	if leaf.Next != "" {
		nextNode, err := bt.storage.LoadNode(leaf.Next)
		if err == nil {
			nextNode.Previous = right.ID
			nextNode.IsDirty = true
			_ = bt.storage.SaveNode(nextNode)
		}
	}
	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]
	leaf.Next = right.ID
	leaf.IsDirty = true
	log.Printf("splitLeaf: left node %s keys=%#v, right node %s keys=%#v", leaf.ID, leaf.Keys, right.ID, right.Keys)
	// Set parent pointers for right node
	right.Parent = leaf.Parent
	if err := bt.storage.SaveNode(leaf); err != nil {
		return err
	}
	if err := bt.storage.SaveNode(right); err != nil {
		return err
	}
	if leaf.Parent == "" {
		log.Printf("splitLeaf: creating new root for split leaves %s and %s", leaf.ID, right.ID)
		// Create new root
		root := NewBTreeNode(generateNodeID(), InternalNode, bt.pageSize, leaf.indexPath)
		// Gather all leaves in order starting from the leftmost
		leaves := []*BTreeNode{}
		current := leaf
		for current.Previous != "" {
			prev, err := bt.storage.LoadNode(current.Previous)
			if err != nil {
				break
			}
			current = prev
		}
		// Now current is the leftmost leaf
		for current != nil {
			leaves = append(leaves, current)
			if current.Next == "" {
				break
			}
			next, err := bt.storage.LoadNode(current.Next)
			if err != nil {
				break
			}
			current = next
		}
		// Set root keys and values
		log.Printf("splitLeaf: new root children order: %v", func() []string {
			ids := []string{}
			for _, n := range leaves {
				ids = append(ids, n.ID+":"+fmt.Sprint(n.Keys))
			}
			return ids
		}())
		root.Values = make([]any, 0, len(leaves))
		root.Keys = make([][]any, 0, len(leaves)-1)
		for i, n := range leaves {
			root.Values = append(root.Values, n.ID)
			if i > 0 {
				root.Keys = append(root.Keys, n.Keys[0])
			}
			n.Parent = root.ID
			_ = bt.storage.SaveNode(n)
		}
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
	log.Printf("splitLeaf: promoted key=%#v to parent %s", right.Keys[0], leaf.Parent)
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
	log.Printf("Search: key=%#v", key)
	if bt.rootID == "" {
		log.Printf("Search: rootID is empty")
		return nil, nil
	}
	node, err := bt.storage.LoadNode(bt.rootID)
	if err != nil {
		log.Printf("Search: failed to load root node %s: %v", bt.rootID, err)
		return nil, err
	}
	// Descend to leftmost leaf if key == nil
	if key == nil {
		for !node.IsLeaf() {
			log.Printf("Search: descending to child %s of internal node %s", node.Values[0], node.ID)
			childID := node.Values[0].(string)
			node, err = bt.storage.LoadNode(childID)
			if err != nil {
				log.Printf("Search: failed to load child node %s: %v", childID, err)
				return nil, err
			}
		}
		// Traverse all leaves using Next pointers, but also ensure we start from the leftmost leaf
		results := []any{}
		visited := map[string]bool{}
		for node != nil && !visited[node.ID] {
			log.Printf("Search: visiting leaf node %s, keys=%#v", node.ID, node.Keys)
			visited[node.ID] = true
			results = append(results, node.Values...)
			if node.Next == "" {
				break
			}
			nextNode, err := bt.storage.LoadNode(node.Next)
			if err != nil {
				log.Printf("Search: failed to load next leaf node %s: %v", node.Next, err)
				break
			}
			node = nextNode
		}
		log.Printf("Search: collected %d results", len(results))
		return results, nil
	}
	// Original search for a specific key
	for !node.IsLeaf() {
		pos := 0
		// Fix: descend to the first child whose separator key is greater than the search key
		for pos < len(node.Keys) && compareKeys(key, node.Keys[pos]) >= 0 {
			pos++
		}
		if pos >= len(node.Values) {
			pos = len(node.Values) - 1
		}
		childID := node.Values[pos].(string)
		log.Printf("Search: descending to child %s of internal node %s for key=%#v", childID, node.ID, key)
		node, err = bt.storage.LoadNode(childID)
		if err != nil {
			log.Printf("Search: failed to load child node %s: %v", childID, err)
			return nil, err
		}
	}
	results := []any{}
	for i, k := range node.Keys {
		if compareKeys(key, k) == 0 {
			results = append(results, node.Values[i])
		}
	}
	log.Printf("Search: found %d results for key=%#v in leaf node %s", len(results), key, node.ID)
	return results, nil
}

// Delete removes all records with the given key from the B+ tree.
func (bt *BTree) Delete(key []any) error {
	if bt.rootID == "" {
		return nil
	}
	root, err := bt.storage.LoadNode(bt.rootID)
	if err != nil {
		return err
	}
	_, shrink, err := bt.deleteRecursive(root, key)
	if err != nil {
		return err
	}
	if shrink && root.IsLeaf() && len(root.Keys) == 0 {
		// Tree is now empty
		bt.rootID = ""
	}
	return nil
}

// deleteRecursive recursively deletes all records with the given key and handles underflow.
// Returns (changed, shrink, error):
//
//	changed: true if the node was modified
//	shrink: true if the subtree height shrank (root merge)
func (bt *BTree) deleteRecursive(node *BTreeNode, key []any) (bool, bool, error) {
	if node.IsLeaf() {
		// Remove all key-value pairs with the matching key
		deleted := false
		for {
			found := false
			for i := 0; i < len(node.Keys); i++ {
				if compareKeys(key, node.Keys[i]) == 0 {
					log.Printf("deleteRecursive: deleting key=%#v at pos=%d in leaf %s", key, i, node.ID)
					node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
					node.Values = append(node.Values[:i], node.Values[i+1:]...)
					deleted = true
					found = true
					break
				}
			}
			if !found {
				break
			}
		}
		if deleted {
			node.IsDirty = true
			_ = bt.storage.SaveNode(node)
		}
		log.Printf("deleteRecursive: after delete, leaf %s keys=%#v, values=%#v", node.ID, node.Keys, node.Values)
		return deleted, len(node.Keys) == 0, nil
	}
	// Internal node: find child
	pos := 0
	// Fix: descend to the first child whose separator key is greater than the delete key
	for pos < len(node.Keys) && compareKeys(key, node.Keys[pos]) >= 0 {
		pos++
	}
	if pos >= len(node.Values) {
		pos = len(node.Values) - 1
	}
	childID := node.Values[pos].(string)
	child, err := bt.storage.LoadNode(childID)
	if err != nil {
		return false, false, err
	}
	changed, shrink, err := bt.deleteRecursive(child, key)
	if err != nil {
		return false, false, err
	}
	if !changed {
		return false, false, nil
	}
	// If child is empty, remove pointer and key
	if shrink {
		if child.IsLeaf() || len(child.Keys) == 0 {
			// Remove child pointer and key (if not leftmost)
			if pos < len(node.Keys) {
				node.Keys = append(node.Keys[:pos], node.Keys[pos+1:]...)
			}
			node.Values = append(node.Values[:pos], node.Values[pos+1:]...)
			node.IsDirty = true
			_ = bt.storage.SaveNode(node)
			// If root and only one child left, promote child
			if node.Parent == "" && len(node.Values) == 1 {
				newRootID := node.Values[0].(string)
				bt.rootID = newRootID
				return true, true, nil
			}
			return true, len(node.Keys) == 0, nil
		}
	}
	_ = bt.storage.SaveNode(node)
	return true, false, nil
}

// Update replaces the value for a given key in a clustered index. Returns error if not unique key index.
func (bt *BTree) Update(key []any, newValue any) error {
	if !bt.isUniqueKey {
		return fmt.Errorf("Update is only supported for clustered (unique key) indexes")
	}
	if bt.rootID == "" {
		return fmt.Errorf("tree is empty")
	}
	node, err := bt.storage.LoadNode(bt.rootID)
	if err != nil {
		return err
	}
	// Descend to the leaf node containing the key
	for !node.IsLeaf() {
		pos := 0
		for pos < len(node.Keys) && compareKeys(key, node.Keys[pos]) > 0 {
			pos++
		}
		childID := node.Values[pos].(string)
		node, err = bt.storage.LoadNode(childID)
		if err != nil {
			return err
		}
	}
	// Find and update the value for the key
	for i, k := range node.Keys {
		if compareKeys(key, k) == 0 {
			node.Values[i] = newValue
			node.IsDirty = true
			return bt.storage.SaveNode(node)
		}
	}
	return fmt.Errorf("key not found: %#v", key)
}

// Utility: compareKeys compares two composite keys.
func compareKeys(a, b []any) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		av := a[i]
		bv := b[i]
		switch va := av.(type) {
		case int:
			switch vb := bv.(type) {
			case int:
				if va < vb {
					return -1
				} else if va > vb {
					return 1
				}
			case float64:
				if float64(va) < vb {
					return -1
				} else if float64(va) > vb {
					return 1
				}
			}
		case float64:
			switch vb := bv.(type) {
			case int:
				if va < float64(vb) {
					return -1
				} else if va > float64(vb) {
					return 1
				}
			case float64:
				if va < vb {
					return -1
				} else if va > vb {
					return 1
				}
			}
		case string:
			vb, ok := bv.(string)
			if ok {
				if va < vb {
					return -1
				} else if va > vb {
					return 1
				}
			}
		default:
			// Fallback to string comparison for uncomparable types (e.g., map[string]any)
			s1 := fmt.Sprintf("%#v", av)
			s2 := fmt.Sprintf("%#v", bv)
			if s1 < s2 {
				return -1
			} else if s1 > s2 {
				return 1
			}
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
