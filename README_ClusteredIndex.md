# ClusteredIndexManager

A B+ tree implementation for FSDB where each node is stored as a separate file on the filesystem. This provides persistent storage and efficient range queries for clustered indexes.

## Features

- **B+ Tree Implementation**: Uses a B+ tree data structure optimized for range queries
- **File-based Storage**: Each node is stored as a JSON file on disk
- **CRUD Operations**: Support for BuildIndex, InsertRow, UpdateRow, and DeleteRow
- **Range Queries**: Efficient range scanning with pagination support
- **Thread Safety**: Uses read-write locks for concurrent access
- **Node Caching**: Intelligent caching with LRU eviction
- **Index Validation**: Built-in consistency checking
- **Metadata Management**: Automatic persistence of index metadata

## Architecture

### Components

1. **BTreeNode** (`btree_node.go`): Represents individual nodes in the B+ tree
2. **ClusteredIndexManager** (`clustered_index_manager.go`): Main manager class
3. **BTree Operations** (`btree_operations.go`): Core B+ tree algorithms
4. **Index Operations** (`index_operations.go`): Search and utility functions

### File Structure

```
/base_path/clustered_index/index_name/
├── metadata.json          # Index metadata
├── 1.json                 # Root node
├── 2.json                 # Child node
├── 3.json                 # Leaf node
└── ...                    # Additional nodes
```

## Usage

### Basic Setup

```go
import "github.com/dannyswat/fsdb"

// Define your schema
schema := fsdb.CollectionSchema{
    Name: "users",
    Columns: []fsdb.ColumnDefinition{
        {FieldName: "id", DataType: datatype.Integer, IsUnique: true},
        {FieldName: "name", DataType: datatype.String},
        {FieldName: "email", DataType: datatype.String},
    },
    ClusteredIndex: fsdb.IndexDefinition{
        Name: "primary",
        Keys: []fsdb.IndexField{
            {Name: "id", Ascending: true},
        },
        IsUnique: true,
        PageSize: 100, // Adjust based on your needs
    },
}

// Create the index manager
cim := fsdb.NewClusteredIndexManager(schema.ClusteredIndex, schema, "/path/to/index")
```

### Building an Index

```go
// Sample data
users := []map[string]any{
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"},
    {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
}

// Build the index (automatically sorts data)
err := cim.BuildIndex(users)
if err != nil {
    log.Fatal(err)
}
```

### CRUD Operations

#### Insert

```go
newUser := map[string]any{
    "id": 4, 
    "name": "David", 
    "email": "david@example.com",
}
err := cim.InsertRow(newUser)
```

#### Find

```go
// Find by primary key
user, err := cim.FindRow([]any{2})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Found user: %v\n", user)
```

#### Update

```go
oldUser := map[string]any{"id": 2, "name": "Bob", "email": "bob@example.com"}
newUser := map[string]any{"id": 2, "name": "Bob Smith", "email": "bob.smith@example.com"}

err := cim.UpdateRow(oldUser, newUser)
```

#### Delete

```go
userToDelete := map[string]any{"id": 3, "name": "Charlie", "email": "charlie@example.com"}
err := cim.DeleteRow(userToDelete)
```

### Range Queries

```go
// Search with options
result, err := cim.Search(fsdb.SearchOptions{
    StartKey:  []any{10},        // Start from ID 10
    EndKey:    []any{50},        // End before ID 50
    Limit:     20,               // Return max 20 results
    Offset:    0,                // Skip 0 results
    Ascending: true,             // Sort ascending
})

if err != nil {
    log.Fatal(err)
}

for _, row := range result.Rows {
    fmt.Printf("ID: %v, Name: %v\n", row["id"], row["name"])
}
```

### Index Management

#### Get Statistics

```go
stats, err := cim.GetStats()
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Nodes: %d, Rows: %d, Height: %d\n", 
    stats.NodeCount, stats.RowCount, stats.Height)
```

#### Validate Index

```go
err := cim.Validate()
if err != nil {
    log.Printf("Index validation failed: %v", err)
} else {
    log.Println("Index is valid")
}
```

#### Close Index

```go
// Save all changes and close
err := cim.Close()
if err != nil {
    log.Fatal(err)
}
```

## Configuration

### Page Size

The `PageSize` in the `IndexDefinition` controls how many keys each node can hold:
- **Smaller values** (e.g., 10-50): More nodes, deeper tree, better for frequent updates
- **Larger values** (e.g., 100-1000): Fewer nodes, shallower tree, better for reads

### Cache Size

The node cache size can be adjusted by modifying `maxCacheSize` in the manager:
- Default: 100 nodes
- Increase for better performance with large datasets
- Decrease to save memory

## Performance Characteristics

- **Insert**: O(log n) - Logarithmic time complexity
- **Search**: O(log n) - Logarithmic time complexity  
- **Range Query**: O(log n + k) - Where k is the number of results
- **Delete**: O(log n) - Logarithmic time complexity
- **Space**: O(n) - Linear space complexity

## Thread Safety

The ClusteredIndexManager is thread-safe and uses read-write locks:
- Multiple concurrent reads are allowed
- Writes are exclusive
- All operations are atomic

## Error Handling

The implementation provides detailed error messages for common scenarios:
- Index corruption
- File I/O errors
- Constraint violations (unique key duplicates)
- Node underflow/overflow conditions

## Example

See `clustered_index_example.go` for a complete working example that demonstrates all features.

## Testing

Run the tests to verify the implementation:

```bash
go test -v
```

## Limitations

1. **File System Dependency**: Performance depends on underlying filesystem
2. **JSON Serialization**: Uses JSON for node storage (could be optimized with binary format)
3. **Memory Usage**: Full nodes are loaded into memory (not page-level caching)
4. **Concurrent Writers**: No support for multiple concurrent writers
5. **Recovery**: No automatic recovery from partial failures

## Future Improvements

- Binary serialization format for better performance
- Write-ahead logging for crash recovery
- Page-level caching instead of node-level
- Compression for stored nodes
- Multi-version concurrency control (MVCC)
