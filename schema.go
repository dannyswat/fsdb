package fsdb

type CollectionSchema struct {
	Name                string             `json:"name"`
	Description         string             `json:"description"`
	Columns             []ColumnDefinition `json:"columns"`
	ClusteredIndex      IndexDefinition    `json:"primary_index"`
	NonClusteredIndexes []IndexDefinition  `json:"indexes"`
}
