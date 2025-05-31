package fsdb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// IndexManager manages a single index (either clustered or non-clustered).
// Each B+ tree node is stored as a file within the index's directory.
type IndexManager struct {
	mu         sync.RWMutex
	indexDef   IndexDefinition
	schema     CollectionSchema // Schema of the collection this index belongs to
	basePath   string           // Base path for the collection's data
	indexPath  string           // Path to this specific index's data (e.g., /basePath/indexes/indexName)
	rootNodeID string           // ID of the root node of the B+ tree
	BTree      *BTree
	Storage    BTreeNodeStorage
	// nodeCache    map[string]*BTreeNode // TODO: Implement node caching
	// nextNodeID   int64                 // TODO: Implement node ID generation
}

// NewIndexManager creates a new IndexManager.
// basePath is the root directory for the collection.
// indexDef is the definition of the index to manage.
// schema is the schema of the collection.
func NewIndexManager(basePath string, indexDef IndexDefinition, schema CollectionSchema) (*IndexManager, error) {
	indexPath := filepath.Join(basePath, "indexes", indexDef.Name)
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return nil, err
	}

	storage := &FileBTreeNodeStorage{IndexPath: indexPath}
	im := &IndexManager{
		indexDef:  indexDef,
		schema:    schema,
		basePath:  basePath,
		indexPath: indexPath,
		Storage:   storage,
	}
	im.BTree = NewBTree(storage, "", indexDef.PageSize)
	return im, nil
}

// GetName returns the name of the index.
func (im *IndexManager) GetName() string {
	return im.indexDef.Name
}

// GetType returns whether the index is clustered or non-clustered.
func (im *IndexManager) GetType() string {
	if im.indexDef.IsClustered {
		return "clustered"
	}
	return "non-clustered"
}

// Build builds the index from scratch using the provided data.
// For a clustered index, 'data' contains full rows.
// For a non-clustered index, 'data' might contain only key values and pointers to the clustered index.
func (im *IndexManager) Build(data []map[string]any) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// Clear existing index files
	d, err := os.ReadDir(im.indexPath)
	if err == nil {
		for _, f := range d {
			os.RemoveAll(filepath.Join(im.indexPath, f.Name()))
		}
	}
	im.BTree = NewBTree(im.Storage, "", im.indexDef.PageSize)

	// Sort data by index keys if needed (not implemented here)
	for _, row := range data {
		var key []any
		if im.indexDef.IsClustered {
			key = extractIndexKey(row, im.indexDef)
		} else {
			key = extractIndexKey(row, im.indexDef)
			// value could be a pointer to clustered index key
		}
		if err := im.BTree.Insert(key, row); err != nil {
			return err
		}
	}
	im.rootNodeID = im.BTree.RootID()
	return nil
}

// Insert inserts a new entry into the index.
// For a clustered index, 'value' is the full row. 'key' is extracted from the value.
// For a non-clustered index, 'key' is the indexed fields, and 'value' is the pointer/reference
// to the clustered index key or row ID.
func (im *IndexManager) Insert(key []any, value any) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	if im.BTree == nil {
		return errors.New("BTree not initialized")
	}
	return im.BTree.Insert(key, value)
}

// Update updates an existing entry in the index.
// This might involve deleting the old entry and inserting the new one,
// especially if the indexed key values change.
func (im *IndexManager) Update(oldKey []any, oldValue any, newKey []any, newValue any) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	// TODO: Implement B+ tree update logic
	// Consider if keys changed. If so, it's a delete and insert.
	// If only non-key values changed (for clustered index) or pointed value changed (for non-clustered),
	// it might be an in-place update if possible or a simpler update.
	return nil // Placeholder
}

// Delete removes an entry from the index.
func (im *IndexManager) Delete(key []any, value any) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	// TODO: Implement B+ tree deletion logic
	return nil // Placeholder
}

// Search finds entries in the index based on a key or a range of keys.
func (im *IndexManager) Search(searchKey []any) ([]any, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()
	if im.BTree == nil {
		return nil, errors.New("BTree not initialized")
	}
	return im.BTree.Search(searchKey)
}

// Helper to extract index key from a row
func extractIndexKey(row map[string]any, def IndexDefinition) []any {
	key := make([]any, len(def.Keys))
	for i, k := range def.Keys {
		key[i] = row[k.Name]
	}
	return key
}

// TODO: Add methods for loading/saving nodes, managing metadata, etc.
