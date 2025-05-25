package fsdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// QueryResult represents a query result with row data and pagination info
type QueryResult struct {
	Rows       []map[string]any `json:"rows"`
	TotalCount int64            `json:"total_count"`
	HasMore    bool             `json:"has_more"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

// SearchOptions defines options for searching the index
type SearchOptions struct {
	StartKey  []any  `json:"start_key,omitempty"` // Inclusive start key
	EndKey    []any  `json:"end_key,omitempty"`   // Exclusive end key
	Limit     int    `json:"limit,omitempty"`     // Maximum number of results
	Offset    int    `json:"offset,omitempty"`    // Number of results to skip
	Cursor    string `json:"cursor,omitempty"`    // Cursor for pagination
	Ascending bool   `json:"ascending"`           // Sort order
}

// Search performs a range query on the clustered index
func (cim *ClusteredIndexManager) Search(options SearchOptions) (*QueryResult, error) {
	cim.mu.RLock()
	defer cim.mu.RUnlock()

	if cim.rootNodeID == "" {
		return &QueryResult{Rows: []map[string]any{}, TotalCount: 0, HasMore: false}, nil
	}

	var startLeaf *BTreeNode
	var err error

	if options.StartKey != nil {
		startLeaf, err = cim.findLeafNode(options.StartKey)
	} else {
		// Find leftmost leaf
		startLeaf, err = cim.findLeftmostLeaf()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to find start leaf: %w", err)
	}

	result := &QueryResult{
		Rows:       make([]map[string]any, 0),
		TotalCount: 0,
		HasMore:    false,
	}

	current := startLeaf
	found := 0
	skipped := 0

	for current != nil {
		for i, key := range current.Keys {
			// Check if we've reached the end key
			if options.EndKey != nil && cim.compareKeys(key, options.EndKey) >= 0 {
				result.HasMore = false
				return result, nil
			}

			// Check if we should start including results
			if options.StartKey == nil || cim.compareKeys(key, options.StartKey) >= 0 {
				// Handle offset
				if skipped < options.Offset {
					skipped++
					continue
				}

				// Check limit
				if options.Limit > 0 && found >= options.Limit {
					result.HasMore = true
					return result, nil
				}

				// Add row to results
				row := current.Values[i].(map[string]any)
				result.Rows = append(result.Rows, row)
				found++
				result.TotalCount++
			}
		}

		// Move to next leaf
		if current.Next == "" {
			break
		}

		current, err = cim.loadNode(current.Next)
		if err != nil {
			return nil, fmt.Errorf("failed to load next leaf: %w", err)
		}
	}

	return result, nil
}

// FindRow finds a specific row by its index key
func (cim *ClusteredIndexManager) FindRow(key []any) (map[string]any, error) {
	cim.mu.RLock()
	defer cim.mu.RUnlock()

	if cim.rootNodeID == "" {
		return nil, fmt.Errorf("index is empty")
	}

	leafNode, err := cim.findLeafNode(key)
	if err != nil {
		return nil, fmt.Errorf("failed to find leaf node: %w", err)
	}

	// Search for exact key match
	for i, nodeKey := range leafNode.Keys {
		if cim.compareKeys(nodeKey, key) == 0 {
			return leafNode.Values[i].(map[string]any), nil
		}
	}

	return nil, fmt.Errorf("row not found")
}

// findLeftmostLeaf finds the leftmost leaf node in the B+ tree
func (cim *ClusteredIndexManager) findLeftmostLeaf() (*BTreeNode, error) {
	if cim.rootNodeID == "" {
		return nil, fmt.Errorf("index is empty")
	}

	current, err := cim.loadNode(cim.rootNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to load root node: %w", err)
	}

	// Traverse down to leftmost leaf
	for !current.IsLeaf() {
		// Always take the first (leftmost) child
		childID := current.Values[0].(string)
		current, err = cim.loadNode(childID)
		if err != nil {
			return nil, fmt.Errorf("failed to load child node %s: %w", childID, err)
		}
	}

	return current, nil
}

// GetStats returns statistics about the clustered index
func (cim *ClusteredIndexManager) GetStats() (*IndexStats, error) {
	cim.mu.RLock()
	defer cim.mu.RUnlock()

	stats := &IndexStats{
		IndexName: cim.indexDef.Name,
		NodeCount: 0,
		RowCount:  0,
		Height:    0,
		PageSize:  cim.indexDef.PageSize,
	}

	if cim.rootNodeID == "" {
		return stats, nil
	}

	// Calculate stats by traversing the tree
	err := cim.calculateStats(cim.rootNodeID, 0, stats)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate stats: %w", err)
	}

	return stats, nil
}

// IndexStats represents statistics about the clustered index
type IndexStats struct {
	IndexName string `json:"index_name"`
	NodeCount int64  `json:"node_count"`
	RowCount  int64  `json:"row_count"`
	Height    int    `json:"height"`
	PageSize  int    `json:"page_size"`
}

// calculateStats recursively calculates index statistics
func (cim *ClusteredIndexManager) calculateStats(nodeID string, depth int, stats *IndexStats) error {
	node, err := cim.loadNode(nodeID)
	if err != nil {
		return err
	}

	stats.NodeCount++
	if depth > stats.Height {
		stats.Height = depth
	}

	if node.IsLeaf() {
		stats.RowCount += int64(len(node.Keys))
	} else {
		// Recursively process child nodes
		for _, childID := range node.Values {
			err := cim.calculateStats(childID.(string), depth+1, stats)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// SaveMetadata saves the index metadata to a file
func (cim *ClusteredIndexManager) SaveMetadata() error {
	cim.mu.RLock()
	defer cim.mu.RUnlock()
	return cim.saveMetadataInternal()
}

// saveMetadataInternal saves metadata without acquiring locks (internal use)
func (cim *ClusteredIndexManager) saveMetadataInternal() error {
	metadata := IndexMeta{
		IndexName:    cim.indexDef.Name,
		RootPageName: cim.rootNodeID,
		RowsCount:    0, // Will be calculated later if needed
		PagesCount:   int64(len(cim.nodeCache)),
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metaPath := filepath.Join(cim.basePath, "metadata.json")
	return os.WriteFile(metaPath, data, 0644)
}

// LoadMetadata loads the index metadata from a file
func (cim *ClusteredIndexManager) LoadMetadata() error {
	cim.mu.Lock()
	defer cim.mu.Unlock()

	metaPath := filepath.Join(cim.basePath, "metadata.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata IndexMeta
	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	cim.rootNodeID = metadata.RootPageName
	return nil
}

// Close flushes all dirty nodes to disk and clears the cache
func (cim *ClusteredIndexManager) Close() error {
	cim.mu.Lock()
	defer cim.mu.Unlock()

	// Save all dirty nodes
	err := cim.saveAllDirtyNodes()
	if err != nil {
		return fmt.Errorf("failed to save dirty nodes: %w", err)
	}

	// Save metadata without acquiring lock (we already have it)
	err = cim.saveMetadataInternal()
	if err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// Clear cache
	cim.nodeCache = make(map[string]*BTreeNode)

	return nil
}

// Validate performs consistency checks on the index
func (cim *ClusteredIndexManager) Validate() error {
	cim.mu.RLock()
	defer cim.mu.RUnlock()

	if cim.rootNodeID == "" {
		return nil // Empty index is valid
	}

	return cim.validateNode(cim.rootNodeID, nil, nil, 0)
}

// validateNode recursively validates a node and its subtree
func (cim *ClusteredIndexManager) validateNode(nodeID string, minKey, maxKey any, depth int) error {
	node, err := cim.loadNode(nodeID)
	if err != nil {
		return fmt.Errorf("failed to load node %s: %w", nodeID, err)
	}

	// Check node structure
	if node.IsLeaf() {
		if len(node.Keys) != len(node.Values) {
			return fmt.Errorf("leaf node %s has mismatched keys and values", nodeID)
		}
	} else {
		if len(node.Values) != len(node.Keys)+1 {
			return fmt.Errorf("internal node %s has invalid key/value ratio", nodeID)
		}
	}

	// Check key ordering
	for i := 1; i < len(node.Keys); i++ {
		if cim.compareKeys(node.Keys[i-1], node.Keys[i]) >= 0 {
			return fmt.Errorf("node %s has keys out of order", nodeID)
		}
	}

	// Check key bounds
	if minKey != nil && len(node.Keys) > 0 {
		if cim.compareKeys(node.Keys[0], minKey) < 0 {
			return fmt.Errorf("node %s violates minimum key constraint", nodeID)
		}
	}
	if maxKey != nil && len(node.Keys) > 0 {
		if cim.compareKeys(node.Keys[len(node.Keys)-1], maxKey) >= 0 {
			return fmt.Errorf("node %s violates maximum key constraint", nodeID)
		}
	}

	// Recursively validate children
	if !node.IsLeaf() {
		for i, childID := range node.Values {
			var childMinKey, childMaxKey any
			if i > 0 {
				childMinKey = node.Keys[i-1]
			}
			if i < len(node.Keys) {
				childMaxKey = node.Keys[i]
			}

			err := cim.validateNode(childID.(string), childMinKey, childMaxKey, depth+1)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
