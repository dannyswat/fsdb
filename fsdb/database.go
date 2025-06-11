package fsdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	errCollectionExists   = errors.New("collection already exists")
	errCollectionNotExist = errors.New("collection does not exist")
	errInvalidCollection  = errors.New("invalid collection")
)

// Database manages collections (schemas and their associated indexes).
// It handles creation, deletion, and modification of collection schemas.
type Database struct {
	mu           sync.RWMutex
	basePath     string                 // Base path where all collections are stored (e.g., /data/mydb)
	collections  map[string]*Collection // Map of collection name to Collection object
	fileProvider IFileProvider          // Injected file provider
}

func (db *Database) loadExistingCollections() error {
	files, err := db.fileProvider.ReadDirectory(db.basePath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			collectionPath := filepath.Join(db.basePath, file.Name())
			exists, err := db.fileProvider.FileExists(collectionPath, "schema.json")
			if err != nil {
				return err
			}
			if !exists {
				continue
			}
			data, err := db.fileProvider.ReadFile(collectionPath, "schema.json")
			if err != nil {
				return err
			}
			var schema CollectionSchema
			if err := json.Unmarshal(data, &schema); err != nil {
				return err
			}
			collection, err := NewCollection(collectionPath, schema)
			if err != nil {
				return err
			}
			db.collections[schema.Name] = collection
		}
	}
	return nil
}

// NewDatabase creates a new CollectionManager.
// basePath is the root directory where all database data will be stored.
func NewDatabase(basePath string) (*Database, error) {
	fileProvider := &FileProvider{}
	if err := fileProvider.CreateDirectory(basePath); err != nil {
		return nil, err
	}
	db := &Database{
		basePath:     basePath,
		collections:  make(map[string]*Collection),
		fileProvider: fileProvider,
	}
	if err := db.loadExistingCollections(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *Database) EnsureCreatedCollection(schema CollectionSchema) error {
	err := db.CreateCollection(schema)
	if err != nil && !errors.Is(err, errCollectionExists) {
		return fmt.Errorf("failed to create collection %s: %w", schema.Name, err)
	}

	return nil
}

// CreateCollection creates a new collection with the given schema.
// It initializes the directory structure for the collection and its indexes.
func (db *Database) CreateCollection(schema CollectionSchema) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	collectionPath := filepath.Join(db.basePath, schema.Name)
	dirExists, err := db.fileProvider.DirectoryExists(collectionPath)
	if err != nil {
		return err
	}
	if dirExists {
		return errCollectionExists
	}
	if err := db.fileProvider.CreateDirectory(collectionPath); err != nil {
		return err
	}
	if err := validateSchema(&schema); err != nil {
		db.fileProvider.DeleteDirectory(collectionPath)
		return err
	}
	schema.ID = uuid.New().String()
	schema.CreatedAt = time.Now()
	schema.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		db.fileProvider.DeleteDirectory(collectionPath)
		return err
	}
	if err := db.fileProvider.WriteFile(collectionPath, "schema.json", data); err != nil {
		db.fileProvider.DeleteDirectory(collectionPath)
		return err
	}
	collection, err := NewCollection(collectionPath, schema)
	if err != nil {
		return err
	}
	db.collections[schema.Name] = collection
	return nil
}

// GetCollectionSchema retrieves the schema for a given collection name.
func (db *Database) GetCollectionSchema(collectionName string) (*CollectionSchema, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	collectionPath := filepath.Join(db.basePath, collectionName)
	exists, err := db.fileProvider.FileExists(collectionPath, "schema.json")
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errCollectionNotExist
	}
	data, err := db.fileProvider.ReadFile(collectionPath, "schema.json")
	if err != nil {
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
func (db *Database) UpdateCollectionSchema(collectionName string, updatedSchema CollectionSchema) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	currentSchema, err := db.GetCollectionSchema(collectionName)
	if err != nil {
		return err
	}
	if currentSchema.Name != updatedSchema.Name || currentSchema.ID != updatedSchema.ID {
		return errInvalidCollection
	}
	updatedSchema.UpdatedAt = time.Now()
	collectionPath := filepath.Join(db.basePath, collectionName)
	data, err := json.MarshalIndent(updatedSchema, "", "  ")
	if err != nil {
		return err
	}
	return db.fileProvider.WriteFile(collectionPath, "schema.json", data)
}

// DeleteCollection removes a collection and all its data.
func (db *Database) DeleteCollection(collectionName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	collectionPath := filepath.Join(db.basePath, collectionName)
	dirExists, err := db.fileProvider.DirectoryExists(collectionPath)
	if err != nil {
		return err
	}
	if !dirExists {
		return errCollectionNotExist
	}
	return db.fileProvider.DeleteDirectory(collectionPath)
}

// GetCollection loads a collection and its indexes by name.
func (db *Database) GetCollection(collectionName string) (*Collection, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	collectionPath := filepath.Join(db.basePath, collectionName)
	schema, err := db.GetCollectionSchema(collectionName)
	if err != nil {
		return nil, err
	}
	return NewCollection(collectionPath, *schema)
}

// validateSchema performs basic validation on a CollectionSchema.
func validateSchema(schema *CollectionSchema) error {
	if schema.Name == "" {
		return errInvalidCollection // Collection name cannot be empty
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
		return errInvalidCollection // Cannot have more than one clustered index
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
	Schema              CollectionSchema
	collectionPath      string
	clusteredIndex      *IndexManager
	nonClusteredIndexes map[string]*IndexManager
	fullTextIndex       *InvertedIndex // Optional full-text index for the collection
}

// NewCollection loads a collection and initializes its indexes.
func NewCollection(collectionPath string, schema CollectionSchema) (*Collection, error) {
	coll := &Collection{
		Schema:              schema,
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
	if schema.EnableFullText {
		ftIndex, err := NewInvertedIndex(filepath.Join(collectionPath, "fulltext"), 3, &FileProvider{})
		if err != nil {
			return nil, err
		}
		coll.fullTextIndex = ftIndex
	}
	return coll, nil
}

// Insert inserts a row into the collection (and all indexes).
func (c *Collection) Insert(row map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusteredIndex == nil {
		return errInvalidCollection
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

	// Update full-text index if enabled
	if c.fullTextIndex != nil {
		docID := c.generateDocumentID(key)
		fullTextContent := c.extractFullTextContent(row)
		if fullTextContent != "" {
			if err := c.fullTextIndex.AddDocument(DocumentID(docID), fullTextContent); err != nil {
				return err
			}
		}
	}

	return nil
}

// Update updates a row in the collection (and all indexes).
func (c *Collection) Update(oldRow, newRow map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusteredIndex == nil {
		return errInvalidCollection
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

	// Update full-text index if enabled
	if c.fullTextIndex != nil {
		oldDocID := c.generateDocumentID(oldKey)
		newDocID := c.generateDocumentID(newKey)

		// Remove old document
		if err := c.fullTextIndex.RemoveDocument(DocumentID(oldDocID)); err != nil {
			return err
		}

		// Add new document
		newFullTextContent := c.extractFullTextContent(newRow)
		if newFullTextContent != "" {
			if err := c.fullTextIndex.AddDocument(DocumentID(newDocID), newFullTextContent); err != nil {
				return err
			}
		}
	}

	return nil
}

// Delete deletes a row from the collection (and all indexes).
func (c *Collection) Delete(row map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusteredIndex == nil {
		return errInvalidCollection
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

	// Remove from full-text index if enabled
	if c.fullTextIndex != nil {
		docID := c.generateDocumentID(key)
		if err := c.fullTextIndex.RemoveDocument(DocumentID(docID)); err != nil {
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
		return nil, errInvalidCollection
	}
	return c.clusteredIndex.Search(key)
}

func (c *Collection) FindByIndex(indexName string, key []any) ([]any, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	im, exists := c.nonClusteredIndexes[indexName]
	if !exists {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}
	return im.Search(key)
}

func (c *Collection) SearchFullText(query string) ([]DocumentID, error) {
	if c.fullTextIndex == nil {
		return nil, errInvalidCollection
	}
	return c.fullTextIndex.Search(query)
}

// generateDocumentID creates a unique document ID from the primary key
func (c *Collection) generateDocumentID(key []any) string {
	// Convert key to string representation
	var keyStr strings.Builder
	for i, val := range key {
		if i > 0 {
			keyStr.WriteString("_")
		}
		keyStr.WriteString(fmt.Sprintf("%v", val))
	}
	return keyStr.String()
}

// extractFullTextContent extracts text from columns marked for full-text indexing
func (c *Collection) extractFullTextContent(row map[string]any) string {
	var textParts []string

	for _, column := range c.Schema.Columns {
		if column.FullText {
			if value, exists := row[column.FieldName]; exists && value != nil {
				textParts = append(textParts, fmt.Sprintf("%v", value))
			}
		}
	}

	return strings.Join(textParts, " ")
}
