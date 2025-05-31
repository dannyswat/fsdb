package fsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestIndexManager_BuildInsertSearch(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "fsdb_test_index")
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	schema := CollectionSchema{
		Name: "testcoll",
		Columns: []ColumnDefinition{
			{FieldName: "id"},
			{FieldName: "name"},
		},
	}
	indexDef := IndexDefinition{
		Name:        "primary",
		IsClustered: true,
		PageSize:    3,
		Keys:        []IndexField{{Name: "id"}},
	}

	im, err := NewIndexManager(tmpDir, indexDef, schema)
	if err != nil {
		t.Fatalf("failed to create IndexManager: %v", err)
	}

	// Build index with initial data
	rows := []map[string]any{
		{"id": 2, "name": "B"},
		{"id": 1, "name": "A"},
		{"id": 3, "name": "C"},
	}
	if err := im.Build(rows); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	results, err := im.Search(nil)
	if err != nil {
		t.Fatalf("Search all failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 rows after build, got %d", len(results))
	}

	// Insert a new row
	if err := im.Insert([]any{4}, map[string]any{"id": 4, "name": "D"}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	results, err = im.Search(nil)
	if err != nil {
		t.Fatalf("Search all failed: %v", err)
	}

	if len(results) != 4 {
		t.Errorf("Expected 4 rows after insert, got %d", len(results))
		t.Errorf("Results: %+v\n", results)
		return
	}

	// Search for a specific key
	results, err = im.Search([]any{2})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 || results[0].(map[string]any)["name"] != "B" {
		t.Errorf("Search for id=2 returned wrong result: %+v", results)
		return
	}

	// Search for a non-existent key
	results, err = im.Search([]any{99})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected no results for id=99, got: %+v", results)
	}

	// Insert duplicate key (should allow, unless unique constraint is enforced)
	err = im.Insert([]any{2}, map[string]any{"id": 2, "name": "B2"})
	if err == nil {
		t.Errorf("Expected error on inserting duplicate key, but got none")
		return
	} else {
		fmt.Printf("Insert duplicate key failed as expected: %v\n", err)
	}

	// Delete a row (should delete all records with key=2)
	err = im.BTree.Delete([]any{2})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	results, err = im.Search([]any{2})
	if err != nil {
		t.Fatalf("Search after delete failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected no results for id=2 after delete, got: %+v", results)
	}

	// Delete the duplicate as well (by key only)
	err = im.BTree.Delete([]any{2})
	if err != nil {
		t.Fatalf("Delete duplicate by key failed: %v", err)
	}
	results, err = im.Search([]any{2})
	if err != nil {
		t.Fatalf("Search after delete duplicate failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected no results for id=2 after deleting all, got: %+v", results)
	}

	// Search for all rows (key=nil)
	results, err = im.Search(nil)
	if err != nil {
		t.Fatalf("Search all failed: %v", err)
	}
	if len(results) < 3 {
		fmt.Printf("Results: %+v\n", results)
		t.Errorf("Expected 3 rows, got %d", len(results))
	}
}
