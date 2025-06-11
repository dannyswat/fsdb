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
	indexPath  string // Path to this specific index's data (e.g., /basePath/indexes/indexName)
	rootNodeID string // ID of the root node of the B+ tree
	bTree      *BTree
	Storage    BTreeNodeStorage
	// nodeCache    map[string]*BTreeNode // TODO: Implement node caching
	// nextNodeID   int64                 // TODO: Implement node ID generation
}

// NewIndexManager creates a new IndexManager.
// basePath is the root directory for the collection.
// indexDef is the definition of the index to manage.
// schema is the schema of the collection.
func NewIndexManager(indexPath string, indexDef IndexDefinition) (*IndexManager, error) {
	storage := &FileBTreeNodeStorage{IndexPath: indexPath}
	if err := storage.Init(); err != nil {
		return nil, err
	}
	im := &IndexManager{
		indexDef:  indexDef,
		indexPath: indexPath,
		Storage:   storage,
	}
	// Load root node ID from meta file if exists
	rootID, err := loadRootNodeID(indexPath)
	if err == nil && rootID != "" {
		im.rootNodeID = rootID
		im.bTree = NewBTree(storage, rootID, indexDef.PageSize, indexDef.IsClustered)
	} else {
		im.bTree = NewBTree(storage, "", indexDef.PageSize, indexDef.IsClustered)
	}
	return im, nil
}

// Helper to persist root node ID to a file
func saveRootNodeID(indexPath, rootID string) error {
	metaPath := filepath.Join(indexPath, "root.meta")
	return os.WriteFile(metaPath, []byte(rootID), 0644)
}

// Helper to load root node ID from a file
func loadRootNodeID(indexPath string) (string, error) {
	metaPath := filepath.Join(indexPath, "root.meta")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return "", err
	}
	return string(data), nil
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
	im.bTree = NewBTree(im.Storage, "", im.indexDef.PageSize, im.indexDef.IsClustered)

	// Sort data by index keys if needed (not implemented here)
	for _, row := range data {
		var key []any
		var value any
		if im.indexDef.IsClustered {
			key = extractIndexKey(row, im.indexDef)
			value = row
		} else {
			key = extractIndexKey(row, im.indexDef)
			value = extractNonClusteredValue(row, im.indexDef)
		}
		if err := im.bTree.Insert(key, value); err != nil {
			return err
		}
	}
	im.rootNodeID = im.bTree.RootID()
	if err := saveRootNodeID(im.indexPath, im.rootNodeID); err != nil {
		return err
	}
	return nil
}

// Insert inserts a new entry into the index.
// For a clustered index, 'value' is the full row. 'key' is extracted from the value.
// For a non-clustered index, 'key' is the indexed fields, and 'value' is the pointer/reference
// to the clustered index key or row ID.
func (im *IndexManager) Insert(key []any, value any) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	if im.bTree == nil {
		return errors.New("BTree not initialized")
	}
	var err error
	if im.indexDef.IsClustered {
		err = im.bTree.Insert(key, value)
	} else {
		row, ok := value.(map[string]any)
		if !ok {
			return errors.New("value must be a map for non-clustered index")
		}
		err = im.bTree.Insert(key, extractNonClusteredValue(row, im.indexDef))
	}
	if err == nil {
		im.rootNodeID = im.bTree.RootID()
		saveRootNodeID(im.indexPath, im.rootNodeID)
	}
	return err
}

// Update updates an existing entry in the index.
// For clustered index: updates the value for the key in-place.
// For non-clustered index: returns an error (not supported).
func (im *IndexManager) Update(oldKey []any, oldValue any, newKey []any, newValue any) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	if im.bTree == nil {
		return errors.New("BTree not initialized")
	}
	var err error
	if im.indexDef.IsClustered {
		if compareKeys(oldKey, newKey) != 0 {
			if err = im.bTree.Delete(oldKey); err == nil {
				err = im.bTree.Insert(newKey, newValue)
			}
		} else {
			err = im.bTree.Update(oldKey, newValue)
		}
	} else {
		_, ok1 := oldValue.(map[string]any)
		newRow, ok2 := newValue.(map[string]any)
		if !ok1 || !ok2 {
			return errors.New("values must be maps for non-clustered index")
		}
		if compareKeys(oldKey, newKey) != 0 {
			if err = im.bTree.Delete(oldKey); err == nil {
				err = im.bTree.Insert(newKey, extractNonClusteredValue(newRow, im.indexDef))
			}
		} else {
			err = im.bTree.Update(oldKey, extractNonClusteredValue(newRow, im.indexDef))
		}
	}
	if err == nil {
		im.rootNodeID = im.bTree.RootID()
		saveRootNodeID(im.indexPath, im.rootNodeID)
	}
	return err
}

// Delete removes an entry from the index.
// For both clustered and non-clustered indexes: deletes all entries with the given key.
func (im *IndexManager) Delete(key []any) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	if im.bTree == nil {
		return errors.New("BTree not initialized")
	}
	err := im.bTree.Delete(key)
	if err == nil {
		im.rootNodeID = im.bTree.RootID()
		saveRootNodeID(im.indexPath, im.rootNodeID)
	}
	return err
}

// Search finds entries in the index based on a key or a range of keys.
func (im *IndexManager) Search(searchKey []any) ([]any, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()
	if im.bTree == nil {
		return nil, errors.New("BTree not initialized")
	}
	return im.bTree.Search(searchKey)
}

// Helper to extract index key from a row
func extractIndexKey(row map[string]any, def IndexDefinition) []any {
	key := make([]any, len(def.Keys))
	for i, k := range def.Keys {
		key[i] = row[k.Name]
	}
	return key
}

// Helper to extract only the primary key and included fields for non-clustered index
func extractNonClusteredValue(row map[string]any, def IndexDefinition) map[string]any {
	result := make(map[string]any)
	// Always include the index key fields (as primary key reference)
	for _, k := range def.Keys {
		if v, ok := row[k.Name]; ok {
			result[k.Name] = v
		}
	}
	// Include additional fields specified in Includes
	for _, k := range def.Includes {
		if v, ok := row[k]; ok {
			result[k] = v
		}
	}
	return result
}
