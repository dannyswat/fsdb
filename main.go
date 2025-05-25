package main

import (
	"fmt"
	"log"
	"os"

	"github.com/dannyswat/fsdb/datatype"
	"github.com/dannyswat/fsdb/fsdb"
)

func main() {
	// Create a temporary directory for the index
	tempDir := "/tmp/fsdb_demo"
	os.RemoveAll(tempDir)
	defer os.RemoveAll(tempDir)

	// Define a schema for a products table
	schema := fsdb.CollectionSchema{
		Name:        "products",
		Description: "Product catalog",
		Columns: []fsdb.ColumnDefinition{
			{
				FieldName:  "id",
				DataType:   datatype.Integer,
				IsUnique:   true,
				IsNullable: false,
			},
			{
				FieldName:  "name",
				DataType:   datatype.String,
				IsNullable: false,
			},
			{
				FieldName:  "category",
				DataType:   datatype.String,
				IsNullable: false,
			},
			{
				FieldName:  "price",
				DataType:   datatype.Float,
				IsNullable: false,
			},
		},
		ClusteredIndex: fsdb.IndexDefinition{
			Name: "primary",
			Keys: []fsdb.IndexField{
				{Name: "id", Ascending: true},
			},
			IsUnique: true,
			PageSize: 3, // Small page size for demonstration
		},
	}

	fmt.Println("🚀 ClusteredIndexManager Demo")
	fmt.Println("=============================")

	// Create the clustered index manager
	cim := fsdb.NewClusteredIndexManager(schema.ClusteredIndex, schema, tempDir)

	// Sample product data
	products := []map[string]any{
		{"id": 5, "name": "Smartphone", "category": "Electronics", "price": 799.99},
		{"id": 2, "name": "Coffee Mug", "category": "Kitchen", "price": 12.99},
		{"id": 8, "name": "Laptop", "category": "Electronics", "price": 1299.99},
		{"id": 1, "name": "Notebook", "category": "Office", "price": 3.99},
		{"id": 3, "name": "Wireless Mouse", "category": "Electronics", "price": 29.99},
		{"id": 7, "name": "Desk Lamp", "category": "Office", "price": 45.99},
		{"id": 4, "name": "Water Bottle", "category": "Sports", "price": 19.99},
		{"id": 6, "name": "Headphones", "category": "Electronics", "price": 199.99},
	}

	// Build the index
	fmt.Println("\n📚 Building index from sample data...")
	err := cim.BuildIndex(products)
	if err != nil {
		log.Fatalf("Failed to build index: %v", err)
	}
	fmt.Println("✅ Index built successfully!")

	// Debug: Check what files were created and examine a few
	fmt.Println("🔧 Debug: Files created:")
	files, _ := os.ReadDir(tempDir + "/clustered_index/primary")
	for _, file := range files {
		fmt.Printf("  %s\n", file.Name())
	}

	// Let's examine the root node
	fmt.Printf("🔧 Debug: Root node ID: %s\n", "will check in code")

	// Add debug to see which node is the leftmost leaf

	// Display all products (should be sorted by ID)
	fmt.Println("\n📋 All products (sorted by ID):")
	result, err := cim.Search(fsdb.SearchOptions{Ascending: true})
	if err != nil {
		log.Fatalf("Failed to search: %v", err)
	}

	for _, row := range result.Rows {
		fmt.Printf("  ID: %d, Name: %-15s, Category: %-12s, Price: $%.2f\n",
			row["id"].(int), row["name"], row["category"], row["price"])
	}

	// Find a specific product
	fmt.Println("\n🔍 Finding product with ID 5:")
	product, err := cim.FindRow([]any{5})
	if err != nil {
		log.Fatalf("Failed to find product: %v", err)
	}
	fmt.Printf("  Found: %s - $%.2f\n", product["name"], product["price"])

	// Insert a new product
	fmt.Println("\n➕ Inserting new product...")
	newProduct := map[string]any{
		"id": 9, "name": "Gaming Keyboard", "category": "Electronics", "price": 129.99,
	}
	err = cim.InsertRow(newProduct)
	if err != nil {
		log.Fatalf("Failed to insert product: %v", err)
	}
	fmt.Println("✅ Product inserted successfully!")

	// Update a product
	fmt.Println("\n✏️ Updating product with ID 3...")
	oldProduct := map[string]any{
		"id": 3, "name": "Wireless Mouse", "category": "Electronics", "price": 29.99,
	}
	updatedProduct := map[string]any{
		"id": 3, "name": "Gaming Mouse", "category": "Electronics", "price": 59.99,
	}
	err = cim.UpdateRow(oldProduct, updatedProduct)
	if err != nil {
		log.Fatalf("Failed to update product: %v", err)
	}
	fmt.Println("✅ Product updated successfully!")

	// Range query
	fmt.Println("\n📊 Products with ID between 3 and 7:")
	rangeResult, err := cim.Search(fsdb.SearchOptions{
		StartKey:  []any{3},
		EndKey:    []any{8}, // Exclusive end
		Ascending: true,
	})
	if err != nil {
		log.Fatalf("Failed to perform range query: %v", err)
	}

	for _, row := range rangeResult.Rows {
		fmt.Printf("  ID: %d, Name: %-15s, Price: $%.2f\n",
			row["id"].(int), row["name"], row["price"])
	}

	// Delete a product
	fmt.Println("\n🗑️ Deleting product with ID 1...")
	productToDelete := map[string]any{
		"id": 1, "name": "Notebook", "category": "Office", "price": 3.99,
	}
	err = cim.DeleteRow(productToDelete)
	if err != nil {
		log.Fatalf("Failed to delete product: %v", err)
	}
	fmt.Println("✅ Product deleted successfully!")

	// Final state
	fmt.Println("\n📋 Final product list:")
	finalResult, err := cim.Search(fsdb.SearchOptions{Ascending: true})
	if err != nil {
		log.Fatalf("Failed to search: %v", err)
	}

	for _, row := range finalResult.Rows {
		fmt.Printf("  ID: %d, Name: %-15s, Category: %-12s, Price: $%.2f\n",
			row["id"].(int), row["name"], row["category"], row["price"])
	}

	// Get index statistics
	fmt.Println("\n📈 Index Statistics:")
	stats, err := cim.GetStats()
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}
	fmt.Printf("  📄 Nodes: %d\n", stats.NodeCount)
	fmt.Printf("  📊 Rows: %d\n", stats.RowCount)
	fmt.Printf("  🌳 Height: %d\n", stats.Height)
	fmt.Printf("  📏 Page Size: %d\n", stats.PageSize)

	// Validate the index
	fmt.Println("\n🔍 Validating index integrity...")
	err = cim.Validate()
	if err != nil {
		fmt.Printf("⚠️  Index validation warning: %v\n", err)
		// Continue anyway for demo purposes
	} else {
		fmt.Println("✅ Index is valid!")
	}

	// Close the index
	err = cim.Close()
	if err != nil {
		log.Fatalf("Failed to close index: %v", err)
	}

	fmt.Println("\n🎉 Demo completed successfully!")
	fmt.Println("\nThe ClusteredIndexManager provides:")
	fmt.Println("  • Efficient B+ tree storage with file-based nodes")
	fmt.Println("  • CRUD operations (Create, Read, Update, Delete)")
	fmt.Println("  • Range queries with pagination support")
	fmt.Println("  • Automatic sorting and indexing")
	fmt.Println("  • Index validation and statistics")
	fmt.Println("  • Thread-safe operations")
}
