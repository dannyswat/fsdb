package fsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dannyswat/fsdb/datatype"
)

func TestDatabase_CreateAndGetCollection(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	schema := CollectionSchema{
		Name: "users",
		Columns: []ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "name", DataType: datatype.String},
		},
		Indexes: []IndexDefinition{
			{
				Name:        "pk_users",
				IsClustered: true,
				Keys:        []IndexField{{Name: "id"}},
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
	if coll.schema.Name != "users" {
		t.Errorf("expected collection name 'users', got %s", coll.schema.Name)
	}
}

func TestDatabase_InsertUpdateDeleteFind(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	schema := CollectionSchema{
		Name: "products",
		Columns: []ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "name", DataType: datatype.String},
			{FieldName: "price", DataType: datatype.Float},
		},
		Indexes: []IndexDefinition{
			{
				Name:        "pk_products",
				IsClustered: true,
				Keys:        []IndexField{{Name: "id"}},
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
	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	schema := CollectionSchema{
		Name: "orders",
		Columns: []ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "desc", DataType: datatype.String},
		},
		Indexes: []IndexDefinition{
			{
				Name:        "pk_orders",
				IsClustered: true,
				Keys:        []IndexField{{Name: "id"}},
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
