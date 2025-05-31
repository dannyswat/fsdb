package fsdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

// CollectionManager manages collections (schemas and their associated indexes).
// It handles creation, deletion, and modification of collection schemas.
type CollectionManager struct {
	mu       sync.RWMutex
	basePath string // Base path where all collections are stored (e.g., /data/mydb)
	// collections map[string]*Collection // Map of collection name to Collection object
}

// NewCollectionManager creates a new CollectionManager.
// basePath is the root directory where all database data will be stored.
func NewCollectionManager(basePath string) (*CollectionManager, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}
	cm := &CollectionManager{
		basePath: basePath,
		// collections: make(map[string]*Collection),
	}
	// TODO: Load existing collection schemas from disk
	return cm, nil
}

// CreateCollection creates a new collection with the given schema.
// It initializes the directory structure for the collection and its indexes.
func (cm *CollectionManager) CreateCollection(schema CollectionSchema) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	collectionPath := filepath.Join(cm.basePath, schema.Name)
	if _, err := os.Stat(collectionPath); !os.IsNotExist(err) {
		return os.ErrExist // Collection already exists
	}

	if err := os.MkdirAll(collectionPath, 0755); err != nil {
		return err
	}

	// Create a subdirectory for indexes
	if err := os.MkdirAll(filepath.Join(collectionPath, "indexes"), 0755); err != nil {
		return err
	}

	// Validate schema (e.g., ensure there's exactly one clustered index)
	if err := validateSchema(&schema); err != nil {
		// Clean up created directory if schema is invalid
		os.RemoveAll(collectionPath)
		return err
	}

	// Save the schema to a file (e.g., schema.json) within the collection's directory
	schema.ID = uuid.New().String() // Assign a unique ID to the schema
	schema.CreatedAt = time.Now()
	schema.UpdatedAt = time.Now()

	schemaFilePath := filepath.Join(collectionPath, "schema.json")
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		os.RemoveAll(collectionPath) // Clean up
		return err
	}
	if err := os.WriteFile(schemaFilePath, data, 0644); err != nil {
		os.RemoveAll(collectionPath) // Clean up
		return err
	}

	// TODO: Initialize IndexManager for the clustered index
	// TODO: Initialize IndexManagers for any non-clustered indexes defined in the schema

	// cm.collections[schema.Name] = NewCollection(collectionPath, schema)
	return nil
}

// GetCollectionSchema retrieves the schema for a given collection name.
func (cm *CollectionManager) GetCollectionSchema(collectionName string) (*CollectionSchema, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	collectionPath := filepath.Join(cm.basePath, collectionName)
	schemaFilePath := filepath.Join(collectionPath, "schema.json")

	data, err := os.ReadFile(schemaFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist // Collection schema not found
		}
		return nil, err
	}

	var schema CollectionSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

// UpdateCollectionSchema updates the schema of an existing collection.
// This can be a complex operation, potentially requiring data migration or re-indexing.
// For now, we'll keep it simple and assume only additive changes or metadata updates.
func (cm *CollectionManager) UpdateCollectionSchema(collectionName string, updatedSchema CollectionSchema) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	currentSchema, err := cm.GetCollectionSchema(collectionName)
	if err != nil {
		return err
	}

	// Basic validation: Name and ID should not change
	if currentSchema.Name != updatedSchema.Name || currentSchema.ID != updatedSchema.ID {
		return os.ErrInvalid // Or a more specific error
	}

	// TODO: Implement more sophisticated schema update logic:
	// - Adding/removing columns (may require data migration)
	// - Adding/removing indexes (may require re-indexing)
	// - Modifying existing column definitions (e.g., changing data type - very complex)

	updatedSchema.UpdatedAt = time.Now()

	collectionPath := filepath.Join(cm.basePath, collectionName)
	schemaFilePath := filepath.Join(collectionPath, "schema.json")
	data, err := json.MarshalIndent(updatedSchema, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(schemaFilePath, data, 0644)
}

// DeleteCollection removes a collection and all its data.
func (cm *CollectionManager) DeleteCollection(collectionName string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	collectionPath := filepath.Join(cm.basePath, collectionName)
	if _, err := os.Stat(collectionPath); os.IsNotExist(err) {
		return os.ErrNotExist // Collection does not exist
	}

	// delete(cm.collections, collectionName)
	return os.RemoveAll(collectionPath)
}

// GetCollection loads a collection and its indexes by name.
func (cm *CollectionManager) GetCollection(collectionName string) (*Collection, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	collectionPath := filepath.Join(cm.basePath, collectionName)
	schema, err := cm.GetCollectionSchema(collectionName)
	if err != nil {
		return nil, err
	}
	return NewCollection(collectionPath, *schema)
}

// validateSchema performs basic validation on a CollectionSchema.
func validateSchema(schema *CollectionSchema) error {
	if schema.Name == "" {
		return os.ErrInvalid // Collection name cannot be empty
	}

	clusteredIndexCount := 0
	for _, idx := range schema.Indexes {
		if idx.IsClustered {
			clusteredIndexCount++
		}
	}

	if clusteredIndexCount == 0 {
		// If no explicit clustered index, and there's a primary key candidate, one could be created by default.
		// For now, require one to be defined or ensure the schema implies one.
		// This part depends on how primary keys are handled if not via a clustered index.
		// Let's assume for now that a clustered index must be explicitly defined if used.
	}

	if clusteredIndexCount > 1 {
		return os.ErrInvalid // Cannot have more than one clustered index
	}

	// TODO: Add more validation rules:
	// - Index names must be unique within the collection.
	// - Index key fields must exist in the collection's columns.
	// - Column names must be unique.
	return nil
}

// Collection represents a loaded collection with its indexes.
type Collection struct {
	mu                  sync.RWMutex
	schema              CollectionSchema
	collectionPath      string
	clusteredIndex      *IndexManager
	nonClusteredIndexes map[string]*IndexManager
}

// NewCollection loads a collection and initializes its indexes.
func NewCollection(collectionPath string, schema CollectionSchema) (*Collection, error) {
	coll := &Collection{
		schema:              schema,
		collectionPath:      collectionPath,
		nonClusteredIndexes: make(map[string]*IndexManager),
	}
	for _, idx := range schema.Indexes {
		im, err := NewIndexManager(filepath.Join(collectionPath, idx.Name), idx)
		if err != nil {
			return nil, err
		}
		if idx.IsClustered {
			coll.clusteredIndex = im
		} else {
			coll.nonClusteredIndexes[idx.Name] = im
		}
	}
	return coll, nil
}

// Insert inserts a row into the collection (and all indexes).
func (c *Collection) Insert(row map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusteredIndex == nil {
		return os.ErrInvalid // No clustered index defined
	}
	key := extractIndexKey(row, c.clusteredIndex.indexDef)
	if err := c.clusteredIndex.Insert(key, row); err != nil {
		return err
	}
	for _, im := range c.nonClusteredIndexes {
		idxKey := extractIndexKey(row, im.indexDef)
		if err := im.Insert(idxKey, row); err != nil {
			return err
		}
	}
	return nil
}

// Update updates a row in the collection (and all indexes).
func (c *Collection) Update(oldRow, newRow map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusteredIndex == nil {
		return os.ErrInvalid
	}
	oldKey := extractIndexKey(oldRow, c.clusteredIndex.indexDef)
	newKey := extractIndexKey(newRow, c.clusteredIndex.indexDef)
	if err := c.clusteredIndex.Update(oldKey, oldRow, newKey, newRow); err != nil {
		return err
	}
	for _, im := range c.nonClusteredIndexes {
		oldIdxKey := extractIndexKey(oldRow, im.indexDef)
		newIdxKey := extractIndexKey(newRow, im.indexDef)
		if err := im.Update(oldIdxKey, oldRow, newIdxKey, newRow); err != nil {
			return err
		}
	}
	return nil
}

// Delete deletes a row from the collection (and all indexes).
func (c *Collection) Delete(row map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusteredIndex == nil {
		return os.ErrInvalid
	}
	key := extractIndexKey(row, c.clusteredIndex.indexDef)
	if err := c.clusteredIndex.Delete(key); err != nil {
		return err
	}
	for _, im := range c.nonClusteredIndexes {
		idxKey := extractIndexKey(row, im.indexDef)
		if err := im.Delete(idxKey); err != nil {
			return err
		}
	}
	return nil
}

// Search finds rows in the collection by key (clustered index).
func (c *Collection) Find(key []any) ([]any, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.clusteredIndex == nil {
		return nil, os.ErrInvalid
	}
	return c.clusteredIndex.Search(key)
}
