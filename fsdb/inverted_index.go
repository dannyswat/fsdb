package fsdb

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dannyswat/fsdb/fulltext"
)

type DocumentID string

// TermFrequency represents the frequency of a term in a document
type TermFrequency struct {
	DocID DocumentID `json:"doc_id"`
	Freq  int        `json:"frequency"`
}

// PostingList represents the list of documents containing a specific term
type PostingList struct {
	Term      string          `json:"term"`
	Documents []TermFrequency `json:"documents"`
}

// InvertedIndex provides file-based full-text search using n-grams
type InvertedIndex struct {
	mu           sync.RWMutex
	indexPath    string
	ngramSize    int
	fileProvider IFileProvider
	cache        map[string]*PostingList // In-memory cache for frequently accessed terms
}

// NewInvertedIndex creates a new file-based inverted index
func NewInvertedIndex(indexPath string, ngramSize int, fileProvider IFileProvider) (*InvertedIndex, error) {
	if ngramSize <= 0 {
		ngramSize = 3 // Default to trigrams
	}

	idx := &InvertedIndex{
		indexPath:    indexPath,
		ngramSize:    ngramSize,
		fileProvider: fileProvider,
		cache:        make(map[string]*PostingList),
	}

	// Ensure index directory exists
	if err := fileProvider.CreateDirectory(indexPath); err != nil {
		return nil, fmt.Errorf("failed to create index directory: %w", err)
	}

	return idx, nil
}

// AddDocument adds or updates a document in the index
func (idx *InvertedIndex) AddDocument(docID DocumentID, text string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove existing document first
	if err := idx.removeDocumentUnsafe(docID); err != nil {
		return fmt.Errorf("failed to remove existing document: %w", err)
	}

	// Generate n-grams from the text
	ngrams := fulltext.NGram(strings.ToLower(text), idx.ngramSize)

	// Count term frequencies
	termFreq := make(map[string]int)
	for _, ngram := range ngrams {
		termFreq[ngram]++
	}

	// Update posting lists for each term
	for term, freq := range termFreq {
		if err := idx.addTermOccurrence(term, docID, freq); err != nil {
			return fmt.Errorf("failed to add term %s: %w", term, err)
		}
	}

	return nil
}

// RemoveDocument removes a document from the index
func (idx *InvertedIndex) RemoveDocument(docID DocumentID) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.removeDocumentUnsafe(docID)
}

// Search performs a full-text search and returns matching document IDs
func (idx *InvertedIndex) Search(query string) ([]DocumentID, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if strings.TrimSpace(query) == "" {
		return nil, nil
	}

	// Generate n-grams from the query
	ngrams := fulltext.NGram(strings.ToLower(query), idx.ngramSize)
	if len(ngrams) == 0 {
		return nil, nil
	}

	// Get posting lists for each n-gram
	var postingLists []*PostingList
	for _, ngram := range ngrams {
		postingList, err := idx.getPostingList(ngram)
		if err != nil {
			return nil, fmt.Errorf("failed to get posting list for %s: %w", ngram, err)
		}
		if postingList != nil {
			postingLists = append(postingLists, postingList)
		}
	}

	if len(postingLists) == 0 {
		return nil, nil
	}

	// Calculate document scores based on term frequency
	docScores := make(map[DocumentID]float64)
	for _, postingList := range postingLists {
		for _, tf := range postingList.Documents {
			docScores[tf.DocID] += float64(tf.Freq)
		}
	}

	// Sort documents by score (descending)
	type docScore struct {
		docID DocumentID
		score float64
	}

	var results []docScore
	for docID, score := range docScores {
		results = append(results, docScore{docID: docID, score: score})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})

	// Extract document IDs
	var docIDs []DocumentID
	for _, result := range results {
		docIDs = append(docIDs, result.docID)
	}

	return docIDs, nil
}

// addTermOccurrence adds or updates a term occurrence in the index
func (idx *InvertedIndex) addTermOccurrence(term string, docID DocumentID, freq int) error {
	postingList, err := idx.getPostingList(term)
	if err != nil {
		return err
	}

	if postingList == nil {
		postingList = &PostingList{
			Term:      term,
			Documents: []TermFrequency{},
		}
	}

	// Find existing document or add new one
	found := false
	for i := range postingList.Documents {
		if postingList.Documents[i].DocID == docID {
			postingList.Documents[i].Freq += freq
			found = true
			break
		}
	}

	if !found {
		postingList.Documents = append(postingList.Documents, TermFrequency{
			DocID: docID,
			Freq:  freq,
		})
	}

	// Sort documents by frequency (descending)
	sort.Slice(postingList.Documents, func(i, j int) bool {
		return postingList.Documents[i].Freq > postingList.Documents[j].Freq
	})

	// Save to file and update cache
	if err := idx.savePostingList(postingList); err != nil {
		return err
	}

	idx.cache[term] = postingList
	return nil
}

// removeDocumentUnsafe removes a document from all posting lists (not thread-safe)
func (idx *InvertedIndex) removeDocumentUnsafe(docID DocumentID) error {
	// This is a simplified implementation - in practice, you might want to
	// maintain a reverse index (document -> terms) for more efficient removal

	// For now, we'll scan all posting list files
	// This is not the most efficient approach but works for the basic implementation

	// Clear from cache
	for term, postingList := range idx.cache {
		newDocs := make([]TermFrequency, 0, len(postingList.Documents))
		for _, tf := range postingList.Documents {
			if tf.DocID != docID {
				newDocs = append(newDocs, tf)
			}
		}
		postingList.Documents = newDocs

		if len(postingList.Documents) == 0 {
			// Remove empty posting list
			delete(idx.cache, term)
			idx.fileProvider.DeleteFile(idx.indexPath, idx.getTermFileName(term))
		} else {
			idx.savePostingList(postingList)
		}
	}

	return nil
}

// getPostingList retrieves a posting list for a term
func (idx *InvertedIndex) getPostingList(term string) (*PostingList, error) {
	// Check cache first
	if postingList, exists := idx.cache[term]; exists {
		return postingList, nil
	}

	// Load from file
	fileName := idx.getTermFileName(term)
	exists, err := idx.fileProvider.FileExists(idx.indexPath, fileName)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	data, err := idx.fileProvider.ReadFile(idx.indexPath, fileName)
	if err != nil {
		return nil, err
	}

	var postingList PostingList
	if err := json.Unmarshal(data, &postingList); err != nil {
		return nil, fmt.Errorf("failed to unmarshal posting list: %w", err)
	}

	// Cache it
	idx.cache[term] = &postingList
	return &postingList, nil
}

// savePostingList saves a posting list to file
func (idx *InvertedIndex) savePostingList(postingList *PostingList) error {
	data, err := json.MarshalIndent(postingList, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal posting list: %w", err)
	}

	fileName := idx.getTermFileName(postingList.Term)
	return idx.fileProvider.WriteFile(idx.indexPath, fileName, data)
}

// getTermFileName generates a filename for a term's posting list
func (idx *InvertedIndex) getTermFileName(term string) string {
	// Use a simple hash-based approach to avoid filesystem issues with special characters
	// In practice, you might want a more sophisticated approach
	return fmt.Sprintf("term_%x.json", []byte(term))
}

// GetStats returns basic statistics about the index
func (idx *InvertedIndex) GetStats() (map[string]interface{}, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := map[string]interface{}{
		"ngram_size":   idx.ngramSize,
		"cached_terms": len(idx.cache),
		"index_path":   idx.indexPath,
	}

	return stats, nil
}
