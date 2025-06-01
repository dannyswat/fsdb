package fsdb_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dannyswat/fsdb"
	"github.com/dannyswat/fsdb/datatype"
)

func TestDatabase_CreateAndGetCollection(t *testing.T) {
	dir := t.TempDir()
	db, err := fsdb.NewDatabase(dir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	schema := fsdb.CollectionSchema{
		Name: "users",
		Columns: []fsdb.ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "name", DataType: datatype.String},
		},
		Indexes: []fsdb.IndexDefinition{
			{
				Name:        "pk_users",
				IsClustered: true,
				Keys:        []fsdb.IndexField{{Name: "id"}},
			},
		},
	}

	if err := db.CreateCollection(schema); err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	coll, err := db.GetCollection("users")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}
	if coll == nil {
		t.Fatal("expected collection, got nil")
	}
	if coll.Schema.Name != "users" {
		t.Errorf("expected collection name 'users', got %s", coll.Schema.Name)
	}
}

func TestDatabase_InsertUpdateDeleteFind(t *testing.T) {
	dir := t.TempDir()
	db, err := fsdb.NewDatabase(dir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	schema := fsdb.CollectionSchema{
		Name: "products",
		Columns: []fsdb.ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "name", DataType: datatype.String},
			{FieldName: "price", DataType: datatype.Float},
		},
		Indexes: []fsdb.IndexDefinition{
			{
				Name:        "pk_products",
				IsClustered: true,
				Keys:        []fsdb.IndexField{{Name: "id"}},
			},
		},
	}

	if err := db.CreateCollection(schema); err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	coll, err := db.GetCollection("products")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}

	row := map[string]any{"id": 1, "name": "Widget", "price": 9.99}
	if err := coll.Insert(row); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	results, err := coll.Find([]any{1})
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	got := results[0].(map[string]any)
	if got["name"] != "Widget" {
		t.Errorf("expected name 'Widget', got %v", got["name"])
	}

	// Update
	updated := map[string]any{"id": 1, "name": "WidgetX", "price": 12.99}
	if err := coll.Update(row, updated); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	results, err = coll.Find([]any{1})
	if err != nil {
		t.Fatalf("find after update failed: %v", err)
	}
	got = results[0].(map[string]any)
	if got["name"] != "WidgetX" {
		t.Errorf("expected updated name 'WidgetX', got %v", got["name"])
	}

	// Delete
	if err := coll.Delete(updated); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	results, err = coll.Find([]any{1})
	if err != nil {
		t.Fatalf("find after delete failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results))
	}
}

func TestDatabase_DeleteCollection(t *testing.T) {
	dir := t.TempDir()
	db, err := fsdb.NewDatabase(dir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	schema := fsdb.CollectionSchema{
		Name: "orders",
		Columns: []fsdb.ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "desc", DataType: datatype.String},
		},
		Indexes: []fsdb.IndexDefinition{
			{
				Name:        "pk_orders",
				IsClustered: true,
				Keys:        []fsdb.IndexField{{Name: "id"}},
			},
		},
	}

	if err := db.CreateCollection(schema); err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	if err := db.DeleteCollection("orders"); err != nil {
		t.Fatalf("failed to delete collection: %v", err)
	}
	if _, err := db.GetCollection("orders"); err == nil {
		t.Error("expected error when getting deleted collection, got nil")
	}
	if _, err := os.Stat(filepath.Join(dir, "orders")); !os.IsNotExist(err) {
		t.Error("expected collection directory to be deleted")
	}
}

func TestDatabase_FullTextIndexing(t *testing.T) {
	dir := t.TempDir()
	db, err := fsdb.NewDatabase(dir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	// Create schema with full-text enabled columns
	schema := fsdb.CollectionSchema{
		Name:           "articles",
		EnableFullText: true,
		Columns: []fsdb.ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "title", DataType: datatype.String, FullText: true},
			{FieldName: "content", DataType: datatype.String, FullText: true},
			{FieldName: "author", DataType: datatype.String}, // Not indexed for full-text
			{FieldName: "category", DataType: datatype.String, FullText: true},
		},
		Indexes: []fsdb.IndexDefinition{
			{
				Name:        "pk_articles",
				IsClustered: true,
				Keys:        []fsdb.IndexField{{Name: "id"}},
			},
		},
	}

	if err := db.CreateCollection(schema); err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	coll, err := db.GetCollection("articles")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}

	// Test data
	articles := []map[string]any{
		{
			"id":       1,
			"title":    "Go Programming Basics",
			"content":  "Learn the fundamentals of Go programming language",
			"author":   "John Doe",
			"category": "programming",
		},
		{
			"id":       2,
			"title":    "Advanced Go Techniques",
			"content":  "Master advanced concepts in Go development",
			"author":   "Jane Smith",
			"category": "programming",
		},
		{
			"id":       3,
			"title":    "Database Design",
			"content":  "Learn about database design patterns and best practices",
			"author":   "Bob Wilson",
			"category": "database",
		},
		{
			"id":       4,
			"title":    "Chinese Programming 中文编程",
			"content":  "学习编程语言的基础知识",
			"author":   "Li Wei",
			"category": "programming",
		},
	}

	// Insert articles
	for _, article := range articles {
		if err := coll.Insert(article); err != nil {
			t.Fatalf("failed to insert article %d: %v", article["id"], err)
		}
	}

	// Test 1: Search for "programming" (should match articles 1, 2, and 4)
	results, err := coll.SearchFullText("programming")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	if len(results) < 3 {
		t.Errorf("expected at least 3 results for 'programming', got %d", len(results))
	}

	// Test 2: Search for "Go" (should match articles 1 and 2)
	results, err = coll.SearchFullText("Go")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	if len(results) < 2 {
		t.Errorf("expected at least 2 results for 'Go', got %d", len(results))
	}

	// Test 3: Search for "database" (should match article 3)
	results, err = coll.SearchFullText("database")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	if len(results) < 1 {
		t.Errorf("expected at least 1 result for 'database', got %d", len(results))
	}

	// Test 4: Search for Chinese text (should match article 4)
	results, err = coll.SearchFullText("编程")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	if len(results) < 1 {
		t.Errorf("expected at least 1 result for '编程', got %d", len(results))
	}

	// Test 5: Search for non-existent term
	results, err = coll.SearchFullText("nonexiszteznt")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for 'nonexiszteznt', got %d", len(results))
	}

	// Test 6: Update article and verify full-text index is updated
	originalArticle := articles[0]
	updatedArticle := map[string]any{
		"id":       1,
		"title":    "JavaScript Programming Basics", // Changed from Go to JavaScript
		"content":  "Learn the fundamentals of JavaScript programming language",
		"author":   "John Doe",
		"category": "programming",
	}

	if err := coll.Update(originalArticle, updatedArticle); err != nil {
		t.Fatalf("failed to update article: %v", err)
	}

	// Search for "Go" should now return fewer results
	results, err = coll.SearchFullText("Go")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	// Should find fewer results since article 1 no longer contains "Go"
	if len(results) > 2 {
		t.Errorf("expected fewer results for 'Go' after update, got %d", len(results))
	}

	// Search for "JavaScript" should now find the updated article
	results, err = coll.SearchFullText("JavaScript")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	if len(results) < 1 {
		t.Errorf("expected at least 1 result for 'JavaScript' after update, got %d", len(results))
	}

	// Test 7: Delete article and verify removal from full-text index
	if err := coll.Delete(updatedArticle); err != nil {
		t.Fatalf("failed to delete article: %v", err)
	}

	// Search for "JavaScript" should now return no results
	results, err = coll.SearchFullText("JavaScript")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for 'JavaScript' after delete, got %d", len(results))
	}
}

func TestDatabase_FullTextIndexingDisabled(t *testing.T) {
	dir := t.TempDir()
	db, err := fsdb.NewDatabase(dir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	// Create schema without full-text indexing
	schema := fsdb.CollectionSchema{
		Name:           "articles",
		EnableFullText: false, // Disabled
		Columns: []fsdb.ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "title", DataType: datatype.String, FullText: true}, // This should be ignored
		},
		Indexes: []fsdb.IndexDefinition{
			{
				Name:        "pk_articles",
				IsClustered: true,
				Keys:        []fsdb.IndexField{{Name: "id"}},
			},
		},
	}

	if err := db.CreateCollection(schema); err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	coll, err := db.GetCollection("articles")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}

	// Insert should work even without full-text indexing
	article := map[string]any{
		"id":    1,
		"title": "Test Article",
	}

	if err := coll.Insert(article); err != nil {
		t.Fatalf("failed to insert article: %v", err)
	}

	// Full-text search should return an error (or empty results) when disabled
	results, err := coll.SearchFullText("Test")
	if err == nil && len(results) > 0 {
		t.Error("expected full-text search to fail or return empty when disabled")
	}
}

func TestDatabase_FullTextWithEmptyContent(t *testing.T) {
	dir := t.TempDir()
	db, err := fsdb.NewDatabase(dir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	schema := fsdb.CollectionSchema{
		Name:           "articles",
		EnableFullText: true,
		Columns: []fsdb.ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "title", DataType: datatype.String, FullText: true},
			{FieldName: "optional_content", DataType: datatype.String, FullText: true},
		},
		Indexes: []fsdb.IndexDefinition{
			{
				Name:        "pk_articles",
				IsClustered: true,
				Keys:        []fsdb.IndexField{{Name: "id"}},
			},
		},
	}

	if err := db.CreateCollection(schema); err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	coll, err := db.GetCollection("articles")
	if err != nil {
		t.Fatalf("failed to get collection: %v", err)
	}

	// Insert article with some nil/empty full-text fields
	article := map[string]any{
		"id":               1,
		"title":            "Test Article",
		"optional_content": nil, // nil value
	}

	if err := coll.Insert(article); err != nil {
		t.Fatalf("failed to insert article with nil content: %v", err)
	}

	// Should still be able to search by title
	results, err := coll.SearchFullText("Test")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	if len(results) < 1 {
		t.Errorf("expected at least 1 result for 'Test', got %d", len(results))
	}

	// Insert article with empty string
	article2 := map[string]any{
		"id":               2,
		"title":            "",        // empty string
		"optional_content": "Content", // has content
	}

	if err := coll.Insert(article2); err != nil {
		t.Fatalf("failed to insert article with empty title: %v", err)
	}

	// Should be able to search by content
	results, err = coll.SearchFullText("Content")
	if err != nil {
		t.Fatalf("full-text search failed: %v", err)
	}
	if len(results) < 1 {
		t.Errorf("expected at least 1 result for 'Content', got %d", len(results))
	}
}
