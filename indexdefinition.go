package fsdb

type IndexDefinition struct {
	Name          string                 `json:"name"`
	Keys          []IndexField           `json:"keys"`
	IsUnique      bool                   `json:"is_unique"`
	Includes      []string               `json:"includes"`
	PartialFilter []EqualFilterCondition `json:"partial_filter"`
	PageSize      int                    `json:"page_size"`
}
