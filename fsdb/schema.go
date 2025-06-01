package fsdb

import (
	"time"
)

type CollectionSchema struct {
	ID             string             `json:"id"`
	Name           string             `json:"name"`
	Description    string             `json:"description"`
	Columns        []ColumnDefinition `json:"columns"`
	Indexes        []IndexDefinition  `json:"indexes"`
	EnableFullText bool               `json:"enable_full_text"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}
