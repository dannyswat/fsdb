package main

import (
	"fmt"

	"github.com/dannyswat/fsdb/datatype"
	"github.com/dannyswat/fsdb/fsdb"
)

func main() {
	db, err := fsdb.NewDatabase("mydb")
	if err != nil {
		panic(err)
	}
	err = db.CreateCollection(fsdb.CollectionSchema{
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
				PageSize:    10,
			},
		},
	})
	if err != nil {
		panic(err)
	}
	collection, err := db.GetCollection("users")
	if err != nil {
		panic(err)
	}
	collection.Insert(map[string]any{"id": 1, "name": "Alice"})
	collection.Insert(map[string]any{"id": 2, "name": "Bob"})
	collection.Insert(map[string]any{"id": 3, "name": "Charlie"})
	collection.Insert(map[string]any{"id": 4, "name": "Diana"})
	fmt.Println("Inserted 4 users")
}
