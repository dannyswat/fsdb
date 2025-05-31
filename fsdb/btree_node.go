package fsdb

// NodeType defines whether a BTreeNode is a Leaf or Internal node.
type NodeType string

const (
	LeafNode     NodeType = "leaf"
	InternalNode NodeType = "internal"
)

// BTreeNode represents a node in the B+ tree.
// Each node is stored as a separate file on disk.
type BTreeNode struct {
	ID        string   `json:"id"`         // Unique identifier for the node (and filename)
	Type      NodeType `json:"type"`       // LeafNode or InternalNode
	IsDirty   bool     `json:"-"`          // True if the node has been modified and needs saving (not persisted in JSON)
	PageSize  int      `json:"pageSize"`   // Maximum number of keys (degree/order related)
	Parent    string   `json:"parentID"`   // ID of the parent node; empty for root
	Keys      [][]any  `json:"keys"`       // Keys stored in the node. Each key is a slice of values for composite keys.
	Values    []any    `json:"values"`     // For Leaf: data records or pointers. For Internal: child node IDs (strings).
	Next      string   `json:"nextID"`     // For LeafNode: ID of the next leaf node; empty if last
	Previous  string   `json:"previousID"` // For LeafNode: ID of the previous leaf node; empty if first
	indexPath string   `json:"-"`          // Path to the index directory (not persisted in JSON)
}

// NewBTreeNode creates a new BTreeNode.
func NewBTreeNode(id string, nodeType NodeType, pageSize int, indexPath string) *BTreeNode {
	return &BTreeNode{
		ID:        id,
		Type:      nodeType,
		PageSize:  pageSize,
		Keys:      make([][]any, 0, pageSize),
		Values:    make([]any, 0, pageSize+1), // Internal nodes can have one more value (child pointer) than keys
		IsDirty:   true,                       // New nodes are considered dirty and need to be saved
		indexPath: indexPath,
	}
}

// IsLeaf returns true if the node is a leaf node.
func (n *BTreeNode) IsLeaf() bool {
	return n.Type == LeafNode
}

// IsFull returns true if the node has reached its maximum capacity for keys.
// For B+ trees, a node is full if it has `PageSize` keys.
// Leaf nodes can hold `PageSize` key-value pairs.
// Internal nodes can hold `PageSize` keys and `PageSize+1` child pointers.
func (n *BTreeNode) IsFull() bool {
	return len(n.Keys) >= n.PageSize
}

// CanBorrow returns true if the node has more than the minimum number of keys/values
// and can lend one to a sibling.
// Minimum keys for a non-root node is ceil(PageSize/2) - 1 for internal, ceil(PageSize/2) for leaf (simplified to PageSize/2)
func (n *BTreeNode) CanBorrow() bool {
	// Simplified: can borrow if more than half full + 1 (ensuring it doesn't go below min after lending)
	// A more precise calculation depends on B+ tree rules for min fill factor.
	// For internal nodes, min keys = ceil(m/2)-1. For leaf, min keys = floor(m/2) or ceil(m/2).
	// Let's use a simple heuristic: more than half full.
	return len(n.Keys) > n.PageSize/2
}
