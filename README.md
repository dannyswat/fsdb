# FSDB: File-based B+ Tree Database Library for Go

FSDB is a lightweight, embeddable database library for Go, featuring a file-based B+ tree index system. It supports clustered and non-clustered indexes, pluggable file providers, and robust schema/index management. All data and indexes are stored on disk, making it suitable for embedded and server-side use cases.

## Features

- **B+ Tree Indexes**: Fast, persistent, file-based B+ tree implementation.
- **Clustered & Non-Clustered Indexes**: Enforce unique keys or allow duplicates.
- **Pluggable File Provider**: Abstracts all file/directory operations for portability and testing.
- **Schema Management**: Define collections, columns, and indexes with JSON schemas.
- **Thread-Safe**: Uses mutexes for safe concurrent access.
- **Comprehensive Tests**: Unit tests for all major operations.

## Installation

Add to your Go project:

```sh
go get github.com/dannyswat/fsdb
```

## Quick Start

```go
package main

import (
	"fmt"
	"time"
	"github.com/dannyswat/fsdb"
    "github.com/dannyswat/fsdb/datatype"
)

func main() {
	// Create or open a database at a directory
	db, err := fsdb.NewDatabase("./mydb")
	if err != nil {
		panic(err)
	}

	// Define a collection schema
	schema := fsdb.CollectionSchema{
		Name: "users",
		Columns: []fsdb.ColumnDefinition{
			{FieldName: "id", DataType: datatype.Integer},
			{FieldName: "name", DataType: datatype.String},
			{FieldName: "created_at", DataType: datatype.DateTime},
		},
		Indexes: []fsdb.IndexDefinition{
			{
				Name:        "pk_users",
				Fields:      []fsdb.IndexField{{FieldName: "id"}},
				IsClustered: true, // Unique primary key
			},
		},
	}

	// Create the collection
	err = db.CreateCollection(schema)
	if err != nil {
		panic(err)
	}

	// Get the collection
	coll, err := db.GetCollection("users")
	if err != nil {
		panic(err)
	}

	// Insert a row
	row := map[string]any{
		"id":         1,
		"name":       "Alice",
		"created_at": time.Now(),
	}
	err = coll.Insert(row)
	if err != nil {
		panic(err)
	}

	// Find by primary key
	results, err := coll.Find([]any{1})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found: %#v\n", results)
}
```

## Schema & Indexes

- Define columns and indexes in `CollectionSchema`.
- Clustered indexes enforce unique keys (like primary keys).
- Non-clustered indexes allow duplicates and can be used for secondary lookups.

## File Provider Abstraction

All file and directory operations go through the `IFileProvider` interface. You can provide your own implementation for custom storage backends or testing.

## Testing

Run all tests:

```sh
go test ./fsdb/...
```

## License

MIT License.
