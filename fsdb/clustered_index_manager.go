package fsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
)

// ClusteredIndexManager manages a B+ tree clustered index where each node is stored as a file
type ClusteredIndexManager struct {
	mu           sync.RWMutex
	indexDef     IndexDefinition
	schema       CollectionSchema
	basePath     string
	rootNodeID   string
	nodeCache    map[string]*BTreeNode
	nextNodeID   int64
	maxCacheSize int
}

// NewClusteredIndexManager creates a new clustered index manager
func NewClusteredIndexManager(indexDef IndexDefinition, schema CollectionSchema, basePath string) *ClusteredIndexManager {
	indexPath := filepath.Join(basePath, "clustered_index", indexDef.Name)
	os.MkdirAll(indexPath, 0755)

	return &ClusteredIndexManager{
		indexDef:     indexDef,
		schema:       schema,
		basePath:     indexPath,
		nodeCache:    make(map[string]*BTreeNode),
		nextNodeID:   1,
		maxCacheSize: 100, // Keep up to 100 nodes in memory
	}
}

// BuildIndex builds the clustered index from scratch
func (cim *ClusteredIndexManager) BuildIndex(rows []map[string]any) error {
	cim.mu.Lock()
	defer cim.mu.Unlock()

	// Clear existing index
	os.RemoveAll(cim.basePath)
	os.MkdirAll(cim.basePath, 0755)
	cim.nodeCache = make(map[string]*BTreeNode)
	cim.nextNodeID = 1

	// Sort rows by index keys
	sortedRows := make([]map[string]any, len(rows))
	copy(sortedRows, rows)

	sort.Slice(sortedRows, func(i, j int) bool {
		return cim.compareRowsByIndexKeys(sortedRows[i], sortedRows[j]) < 0
	})

	// Create root node
	rootID := cim.generateNodeID()
	cim.rootNodeID = rootID
	rootNode := NewLeafNode(rootID, cim.indexDef.PageSize)
	cim.nodeCache[rootID] = rootNode

	// Insert sorted rows into B+ tree
	for _, row := range sortedRows {
		err := cim.insertIntoNode(rootNode, cim.extractIndexKey(row), row)
		if err != nil {
			return fmt.Errorf("failed to insert row during build: %w", err)
		}
	}

	// Save all dirty nodes
	return cim.saveAllDirtyNodes()
}

// InsertRow inserts a new row into the clustered index
func (cim *ClusteredIndexManager) InsertRow(row map[string]any) error {
	cim.mu.Lock()
	defer cim.mu.Unlock()

	if cim.rootNodeID == "" {
		// Initialize with first row
		rootID := cim.generateNodeID()
		cim.rootNodeID = rootID
		rootNode := NewLeafNode(rootID, cim.indexDef.PageSize)
		cim.nodeCache[rootID] = rootNode
	}

	rootNode, err := cim.loadNode(cim.rootNodeID)
	if err != nil {
		return fmt.Errorf("failed to load root node: %w", err)
	}

	key := cim.extractIndexKey(row)
	err = cim.insertIntoNode(rootNode, key, row)
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	return cim.saveAllDirtyNodes()
}

// UpdateRow updates an existing row in the clustered index
func (cim *ClusteredIndexManager) UpdateRow(oldRow, newRow map[string]any) error {
	cim.mu.Lock()
	defer cim.mu.Unlock()

	// Delete old row
	err := cim.deleteRowInternal(oldRow)
	if err != nil {
		return fmt.Errorf("failed to delete old row during update: %w", err)
	}

	// Insert new row
	rootNode, err := cim.loadNode(cim.rootNodeID)
	if err != nil {
		return fmt.Errorf("failed to load root node for update: %w", err)
	}

	key := cim.extractIndexKey(newRow)
	err = cim.insertIntoNode(rootNode, key, newRow)
	if err != nil {
		return fmt.Errorf("failed to insert updated row: %w", err)
	}

	return cim.saveAllDirtyNodes()
}

// DeleteRow deletes a row from the clustered index
func (cim *ClusteredIndexManager) DeleteRow(row map[string]any) error {
	cim.mu.Lock()
	defer cim.mu.Unlock()

	err := cim.deleteRowInternal(row)
	if err != nil {
		return fmt.Errorf("failed to delete row: %w", err)
	}

	return cim.saveAllDirtyNodes()
}

// deleteRowInternal performs the actual deletion (assumes lock is held)
func (cim *ClusteredIndexManager) deleteRowInternal(row map[string]any) error {
	if cim.rootNodeID == "" {
		return fmt.Errorf("index is empty")
	}

	key := cim.extractIndexKey(row)
	leafNode, err := cim.findLeafNode(key)
	if err != nil {
		return fmt.Errorf("failed to find leaf node: %w", err)
	}

	// Find and remove the key-value pair
	found := false
	for i, nodeKey := range leafNode.Keys {
		if cim.compareKeys(nodeKey, key) == 0 {
			// Remove key and value
			leafNode.Keys = append(leafNode.Keys[:i], leafNode.Keys[i+1:]...)
			leafNode.Values = append(leafNode.Values[:i], leafNode.Values[i+1:]...)
			leafNode.IsDirty = true
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("row not found in index")
	}

	// Handle underflow if necessary
	if len(leafNode.Keys) < cim.indexDef.PageSize/2 && leafNode.ID != cim.rootNodeID {
		return cim.handleUnderflow(leafNode)
	}

	return nil
}

// generateNodeID generates a unique node ID
func (cim *ClusteredIndexManager) generateNodeID() string {
	id := strconv.FormatInt(cim.nextNodeID, 10)
	cim.nextNodeID++
	return id
}

// loadNode loads a node from cache or file
func (cim *ClusteredIndexManager) loadNode(nodeID string) (*BTreeNode, error) {
	if node, exists := cim.nodeCache[nodeID]; exists {
		return node, nil
	}

	node, err := LoadNodeFromFile(cim.basePath, nodeID)
	if err != nil {
		return nil, err
	}

	// Add to cache (with simple LRU eviction)
	if len(cim.nodeCache) >= cim.maxCacheSize {
		// Simple eviction: remove first item
		for id := range cim.nodeCache {
			delete(cim.nodeCache, id)
			break
		}
	}

	cim.nodeCache[nodeID] = node
	return node, nil
}

// extractIndexKey extracts the index key from a row
func (cim *ClusteredIndexManager) extractIndexKey(row map[string]any) []any {
	key := make([]any, len(cim.indexDef.Keys))
	for i, keyField := range cim.indexDef.Keys {
		key[i] = row[keyField.Name]
	}
	return key
}

// compareKeys compares two index keys
func (cim *ClusteredIndexManager) compareKeys(key1, key2 any) int {
	k1, ok1 := key1.([]any)
	k2, ok2 := key2.([]any)

	if !ok1 || !ok2 {
		// Handle single value keys
		return cim.compareValues(key1, key2)
	}

	for i := 0; i < len(k1) && i < len(k2); i++ {
		cmp := cim.compareValues(k1[i], k2[i])
		if cmp != 0 {
			// Apply ascending/descending order
			if i < len(cim.indexDef.Keys) && !cim.indexDef.Keys[i].Ascending {
				cmp = -cmp
			}
			return cmp
		}
	}

	return len(k1) - len(k2)
}

// compareValues compares two individual values
func (cim *ClusteredIndexManager) compareValues(v1, v2 any) int {
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil {
		return -1
	}
	if v2 == nil {
		return 1
	}

	// Use reflection for type-safe comparison
	rv1 := reflect.ValueOf(v1)
	rv2 := reflect.ValueOf(v2)

	if rv1.Type() != rv2.Type() {
		// Try to convert to string for comparison
		s1 := fmt.Sprintf("%v", v1)
		s2 := fmt.Sprintf("%v", v2)
		if s1 < s2 {
			return -1
		} else if s1 > s2 {
			return 1
		}
		return 0
	}

	switch rv1.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i1, i2 := rv1.Int(), rv2.Int()
		if i1 < i2 {
			return -1
		} else if i1 > i2 {
			return 1
		}
		return 0
	case reflect.Float32, reflect.Float64:
		f1, f2 := rv1.Float(), rv2.Float()
		if f1 < f2 {
			return -1
		} else if f1 > f2 {
			return 1
		}
		return 0
	case reflect.String:
		s1, s2 := rv1.String(), rv2.String()
		if s1 < s2 {
			return -1
		} else if s1 > s2 {
			return 1
		}
		return 0
	default:
		// Fallback to string comparison
		s1 := fmt.Sprintf("%v", v1)
		s2 := fmt.Sprintf("%v", v2)
		if s1 < s2 {
			return -1
		} else if s1 > s2 {
			return 1
		}
		return 0
	}
}

// compareRowsByIndexKeys compares two rows by their index keys
func (cim *ClusteredIndexManager) compareRowsByIndexKeys(row1, row2 map[string]any) int {
	key1 := cim.extractIndexKey(row1)
	key2 := cim.extractIndexKey(row2)
	return cim.compareKeys(key1, key2)
}
