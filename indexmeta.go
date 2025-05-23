package fsdb

type IndexMeta struct {
	IndexName    string `json:"index_name"`
	RootPageName string `json:"root_page_name"`
	RowsCount    int64  `json:"rows_count"`
	PagesCount   int64  `json:"pages_count"`
}
