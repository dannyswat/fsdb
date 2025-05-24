package fsdb

import "fmt"

// insertIntoNode inserts a key-value pair into a node, handling splits as necessary
func (cim *ClusteredIndexManager) insertIntoNode(node *BTreeNode, key []any, value any) error {
	if node.IsLeaf() {
		return cim.insertIntoLeaf(node, key, value)
	}
	return cim.insertIntoInternal(node, key, value)
}

// insertIntoLeaf inserts into a leaf node
func (cim *ClusteredIndexManager) insertIntoLeaf(leaf *BTreeNode, key []any, value any) error {
	// Find insertion position
	pos := 0
	for pos < len(leaf.Keys) {
		if cmp := cim.compareKeys(key, leaf.Keys[pos]); cmp < 0 {
			break
		} else if cmp == 0 && cim.indexDef.IsUnique {
			return fmt.Errorf("duplicate key not allowed in unique index")
		}
		pos++
	}

	// Insert key and value
	leaf.Keys = append(leaf.Keys[:pos], append([]any{key}, leaf.Keys[pos:]...)...)
	leaf.Values = append(leaf.Values[:pos], append([]any{value}, leaf.Values[pos:]...)...)
	leaf.IsDirty = true

	// Check if split is needed
	if leaf.IsFull() {
		return cim.splitLeafNode(leaf)
	}

	return nil
}

// insertIntoInternal inserts into an internal node
func (cim *ClusteredIndexManager) insertIntoInternal(internal *BTreeNode, key []any, value any) error {
	// Find child to descend to
	childIndex := 0
	for childIndex < len(internal.Keys) {
		if cim.compareKeys(key, internal.Keys[childIndex]) < 0 {
			break
		}
		childIndex++
	}

	// Load child node
	childID := internal.Values[childIndex].(string)
	child, err := cim.loadNode(childID)
	if err != nil {
		return fmt.Errorf("failed to load child node %s: %w", childID, err)
	}

	// Recursively insert into child
	return cim.insertIntoNode(child, key, value)
}

// splitLeafNode splits a full leaf node
func (cim *ClusteredIndexManager) splitLeafNode(leaf *BTreeNode) error {
	mid := len(leaf.Keys) / 2

	// Create new right node
	rightID := cim.generateNodeID()
	right := NewLeafNode(rightID, leaf.PageSize)
	right.Keys = append(right.Keys, leaf.Keys[mid:]...)
	right.Values = append(right.Values, leaf.Values[mid:]...)
	right.Parent = leaf.Parent
	right.Next = leaf.Next
	right.Previous = leaf.ID

	// Update leaf (becomes left node)
	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]
	leaf.Next = rightID
	leaf.IsDirty = true

	// Update next node's previous pointer
	if right.Next != "" {
		nextNode, err := cim.loadNode(right.Next)
		if err == nil {
			nextNode.Previous = rightID
			nextNode.IsDirty = true
		}
	}

	// Cache the new node
	cim.nodeCache[rightID] = right

	// Promote middle key to parent
	promoteKey := right.Keys[0]
	return cim.promoteKey(leaf, promoteKey, rightID)
}

// splitInternalNode splits a full internal node
func (cim *ClusteredIndexManager) splitInternalNode(internal *BTreeNode) error {
	mid := len(internal.Keys) / 2

	// Create new right node
	rightID := cim.generateNodeID()
	right := NewInternalNode(rightID, internal.PageSize)

	// The middle key goes up to parent, so right node gets keys after middle
	right.Keys = append(right.Keys, internal.Keys[mid+1:]...)
	right.Values = append(right.Values, internal.Values[mid+1:]...)
	right.Parent = internal.Parent

	// Update children's parent pointers
	for _, childID := range right.Values {
		if childNode, err := cim.loadNode(childID.(string)); err == nil {
			childNode.Parent = rightID
			childNode.IsDirty = true
		}
	}

	// Promote key and update internal node
	promoteKey := internal.Keys[mid]
	internal.Keys = internal.Keys[:mid]
	internal.Values = internal.Values[:mid+1] // Keep one more value than keys
	internal.IsDirty = true

	// Cache the new node
	cim.nodeCache[rightID] = right

	return cim.promoteKey(internal, promoteKey, rightID)
}

// promoteKey promotes a key to the parent node
func (cim *ClusteredIndexManager) promoteKey(node *BTreeNode, key any, rightNodeID string) error {
	if node.Parent == "" {
		// Create new root
		newRootID := cim.generateNodeID()
		newRoot := NewInternalNode(newRootID, node.PageSize)
		newRoot.Keys = append(newRoot.Keys, key)
		newRoot.Values = append(newRoot.Values, node.ID, rightNodeID)

		// Update parent pointers
		node.Parent = newRootID
		node.IsDirty = true
		if rightNode, err := cim.loadNode(rightNodeID); err == nil {
			rightNode.Parent = newRootID
			rightNode.IsDirty = true
		}

		cim.rootNodeID = newRootID
		cim.nodeCache[newRootID] = newRoot
		return nil
	}

	// Load parent and insert promoted key
	parent, err := cim.loadNode(node.Parent)
	if err != nil {
		return fmt.Errorf("failed to load parent node: %w", err)
	}

	// Find insertion position in parent
	pos := 0
	for pos < len(parent.Keys) {
		if cim.compareKeys(key, parent.Keys[pos]) < 0 {
			break
		}
		pos++
	}

	// Insert promoted key and right node ID
	parent.Keys = append(parent.Keys[:pos], append([]any{key}, parent.Keys[pos:]...)...)
	parent.Values = append(parent.Values[:pos+1], append([]any{rightNodeID}, parent.Values[pos+1:]...)...)
	parent.IsDirty = true

	// Check if parent needs to be split
	if parent.IsFull() {
		return cim.splitInternalNode(parent)
	}

	return nil
}

// findLeafNode finds the leaf node where a key should be located
func (cim *ClusteredIndexManager) findLeafNode(key []any) (*BTreeNode, error) {
	if cim.rootNodeID == "" {
		return nil, fmt.Errorf("index is empty")
	}

	current, err := cim.loadNode(cim.rootNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to load root node: %w", err)
	}

	// Traverse down to leaf
	for !current.IsLeaf() {
		// Find child to descend to
		childIndex := 0
		for childIndex < len(current.Keys) {
			if cim.compareKeys(key, current.Keys[childIndex]) < 0 {
				break
			}
			childIndex++
		}

		childID := current.Values[childIndex].(string)
		current, err = cim.loadNode(childID)
		if err != nil {
			return nil, fmt.Errorf("failed to load child node %s: %w", childID, err)
		}
	}

	return current, nil
}

// handleUnderflow handles node underflow after deletion
func (cim *ClusteredIndexManager) handleUnderflow(node *BTreeNode) error {
	if node.Parent == "" {
		// Root node can have fewer keys
		return nil
	}

	parent, err := cim.loadNode(node.Parent)
	if err != nil {
		return fmt.Errorf("failed to load parent node: %w", err)
	}

	// Find node's position in parent
	nodePos := -1
	for i, childID := range parent.Values {
		if childID.(string) == node.ID {
			nodePos = i
			break
		}
	}

	if nodePos == -1 {
		return fmt.Errorf("node not found in parent")
	}

	// Try to borrow from left sibling
	if nodePos > 0 {
		leftSiblingID := parent.Values[nodePos-1].(string)
		leftSibling, err := cim.loadNode(leftSiblingID)
		if err == nil && len(leftSibling.Keys) > cim.indexDef.PageSize/2 {
			return cim.borrowFromLeftSibling(node, leftSibling, parent, nodePos)
		}
	}

	// Try to borrow from right sibling
	if nodePos < len(parent.Values)-1 {
		rightSiblingID := parent.Values[nodePos+1].(string)
		rightSibling, err := cim.loadNode(rightSiblingID)
		if err == nil && len(rightSibling.Keys) > cim.indexDef.PageSize/2 {
			return cim.borrowFromRightSibling(node, rightSibling, parent, nodePos)
		}
	}

	// Merge with a sibling
	if nodePos > 0 {
		leftSiblingID := parent.Values[nodePos-1].(string)
		leftSibling, err := cim.loadNode(leftSiblingID)
		if err == nil {
			return cim.mergeWithLeftSibling(node, leftSibling, parent, nodePos)
		}
	}

	if nodePos < len(parent.Values)-1 {
		rightSiblingID := parent.Values[nodePos+1].(string)
		rightSibling, err := cim.loadNode(rightSiblingID)
		if err == nil {
			return cim.mergeWithRightSibling(node, rightSibling, parent, nodePos)
		}
	}

	return fmt.Errorf("unable to handle underflow")
}

// borrowFromLeftSibling borrows a key from the left sibling
func (cim *ClusteredIndexManager) borrowFromLeftSibling(node, leftSibling, parent *BTreeNode, nodePos int) error {
	if node.IsLeaf() {
		// Move last key-value from left sibling to front of node
		borrowedKey := leftSibling.Keys[len(leftSibling.Keys)-1]
		borrowedValue := leftSibling.Values[len(leftSibling.Values)-1]

		leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
		leftSibling.Values = leftSibling.Values[:len(leftSibling.Values)-1]
		leftSibling.IsDirty = true

		node.Keys = append([]any{borrowedKey}, node.Keys...)
		node.Values = append([]any{borrowedValue}, node.Values...)
		node.IsDirty = true

		// Update parent key
		parent.Keys[nodePos-1] = node.Keys[0]
		parent.IsDirty = true
	}

	return nil
}

// borrowFromRightSibling borrows a key from the right sibling
func (cim *ClusteredIndexManager) borrowFromRightSibling(node, rightSibling, parent *BTreeNode, nodePos int) error {
	if node.IsLeaf() {
		// Move first key-value from right sibling to end of node
		borrowedKey := rightSibling.Keys[0]
		borrowedValue := rightSibling.Values[0]

		rightSibling.Keys = rightSibling.Keys[1:]
		rightSibling.Values = rightSibling.Values[1:]
		rightSibling.IsDirty = true

		node.Keys = append(node.Keys, borrowedKey)
		node.Values = append(node.Values, borrowedValue)
		node.IsDirty = true

		// Update parent key
		parent.Keys[nodePos] = rightSibling.Keys[0]
		parent.IsDirty = true
	}

	return nil
}

// mergeWithLeftSibling merges node with its left sibling
func (cim *ClusteredIndexManager) mergeWithLeftSibling(node, leftSibling, parent *BTreeNode, nodePos int) error {
	// Merge node into left sibling
	leftSibling.Keys = append(leftSibling.Keys, node.Keys...)
	leftSibling.Values = append(leftSibling.Values, node.Values...)

	if node.IsLeaf() {
		leftSibling.Next = node.Next
		if node.Next != "" {
			if nextNode, err := cim.loadNode(node.Next); err == nil {
				nextNode.Previous = leftSibling.ID
				nextNode.IsDirty = true
			}
		}
	}

	leftSibling.IsDirty = true

	// Remove node reference from parent
	parent.Keys = append(parent.Keys[:nodePos-1], parent.Keys[nodePos:]...)
	parent.Values = append(parent.Values[:nodePos], parent.Values[nodePos+1:]...)
	parent.IsDirty = true

	// Delete node file and remove from cache
	node.DeleteFile(cim.basePath)
	delete(cim.nodeCache, node.ID)

	// Handle parent underflow
	if len(parent.Keys) < cim.indexDef.PageSize/2 && parent.ID != cim.rootNodeID {
		return cim.handleUnderflow(parent)
	}

	return nil
}

// mergeWithRightSibling merges node with its right sibling
func (cim *ClusteredIndexManager) mergeWithRightSibling(node, rightSibling, parent *BTreeNode, nodePos int) error {
	// Merge right sibling into node
	node.Keys = append(node.Keys, rightSibling.Keys...)
	node.Values = append(node.Values, rightSibling.Values...)

	if node.IsLeaf() {
		node.Next = rightSibling.Next
		if rightSibling.Next != "" {
			if nextNode, err := cim.loadNode(rightSibling.Next); err == nil {
				nextNode.Previous = node.ID
				nextNode.IsDirty = true
			}
		}
	}

	node.IsDirty = true

	// Remove right sibling reference from parent
	parent.Keys = append(parent.Keys[:nodePos], parent.Keys[nodePos+1:]...)
	parent.Values = append(parent.Values[:nodePos+1], parent.Values[nodePos+2:]...)
	parent.IsDirty = true

	// Delete right sibling file and remove from cache
	rightSibling.DeleteFile(cim.basePath)
	delete(cim.nodeCache, rightSibling.ID)

	// Handle parent underflow
	if len(parent.Keys) < cim.indexDef.PageSize/2 && parent.ID != cim.rootNodeID {
		return cim.handleUnderflow(parent)
	}

	return nil
}

// saveAllDirtyNodes saves all dirty nodes to disk
func (cim *ClusteredIndexManager) saveAllDirtyNodes() error {
	for _, node := range cim.nodeCache {
		if node.IsDirty {
			err := node.SaveToFile(cim.basePath)
			if err != nil {
				return fmt.Errorf("failed to save node %s: %w", node.ID, err)
			}
		}
	}
	return nil
}
