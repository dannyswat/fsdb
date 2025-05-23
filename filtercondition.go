package fsdb

type EqualFilterCondition struct {
	Field  string `json:"field"`
	Values []any  `json:"values"`
}
