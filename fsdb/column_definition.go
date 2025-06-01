package fsdb

import "github.com/dannyswat/fsdb/datatype"

type ColumnDefinition struct {
	FieldName     string            `json:"field_name"`
	DataType      datatype.DataType `json:"data_type"`
	IsUnique      bool              `json:"is_unique"`
	IsNullable    bool              `json:"is_nullable"`
	DefaultValue  any               `json:"default_value"`
	AutoIncrement bool              `json:"auto_increment"`
	FullText      bool              `json:"full_text"` // Indicates if the column is indexed for full-text search
	Comment       string            `json:"comment"`
}
