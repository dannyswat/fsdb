package fulltext

import (
	"reflect"
	"sort"
	"testing"
)

func TestNGram_Basic(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		n        int
		expected []string
	}{
		{
			name:     "Basic English trigrams",
			input:    "hello world",
			n:        3,
			expected: []string{"hel", "ell", "llo", "wor", "orl", "rld"},
		},
		{
			name:     "Short English words",
			input:    "go is fun",
			n:        3,
			expected: []string{"go", "fun"}, // "is" is a stop word
		},
		{
			name:     "Stop words filtered",
			input:    "the quick brown fox",
			n:        3,
			expected: []string{"qui", "uic", "ick", "bro", "row", "own", "fox"},
		},
		{
			name:     "Chinese bigrams",
			input:    "你好世界",
			n:        3,
			expected: []string{"你好", "好世", "世界"}, // Chinese always uses bigrams
		},
		{
			name:     "Mixed English and Chinese",
			input:    "Hello 世界 programming",
			n:        3,
			expected: []string{"hel", "ell", "llo", "世界", "pro", "rog", "ogr", "gra", "ram", "amm", "mmi", "min", "ing"},
		},
		{
			name:     "Single character Chinese",
			input:    "我",
			n:        3,
			expected: []string{"我"},
		},
		{
			name:     "Empty input",
			input:    "",
			n:        3,
			expected: []string{},
		},
		{
			name:     "Punctuation handling",
			input:    "hello, world!",
			n:        3,
			expected: []string{"hel", "ell", "llo", "wor", "orl", "rld"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NGram(tt.input, tt.n)

			// Handle nil vs empty slice comparison
			if len(result) == 0 && len(tt.expected) == 0 {
				return // Both are empty, test passes
			}

			// Sort both slices for comparison
			sort.Strings(result)
			sort.Strings(tt.expected)

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("NGram() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNGram_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		n        int
		expected []string
	}{
		{
			name:     "Zero n",
			input:    "hello",
			n:        0,
			expected: nil,
		},
		{
			name:     "Negative n",
			input:    "hello",
			n:        -1,
			expected: nil,
		},
		{
			name:     "Single character English",
			input:    "a",
			n:        3,
			expected: []string{}, // "a" is a stop word and will be filtered
		},
		{
			name:     "Only punctuation",
			input:    "!@#$%",
			n:        3,
			expected: []string{},
		},
		{
			name:     "Only spaces",
			input:    "   ",
			n:        3,
			expected: []string{},
		},
		{
			name:     "Multiple spaces",
			input:    "hello    world",
			n:        3,
			expected: []string{"hel", "ell", "llo", "wor", "orl", "rld"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NGram(tt.input, tt.n)

			// Handle nil vs empty slice comparison
			if (result == nil && tt.expected == nil) || (len(result) == 0 && len(tt.expected) == 0) {
				return // Both are empty/nil, test passes
			}

			// Sort both slices for comparison
			sort.Strings(result)
			sort.Strings(tt.expected)

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("NGram() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExtractWords(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Simple English words",
			input:    "hello world",
			expected: []string{"hello", "world"},
		},
		{
			name:     "Stop words filtered",
			input:    "the quick brown fox",
			expected: []string{"quick", "brown", "fox"},
		},
		{
			name:     "Chinese words",
			input:    "你好世界",
			expected: []string{"你好世界"},
		},
		{
			name:     "Mixed languages",
			input:    "Hello 世界 from 中国",
			expected: []string{"hello", "世界", "中国"}, // "from" is a stop word and filtered
		},
		{
			name:     "Punctuation separated",
			input:    "hello, world! how are you?",
			expected: []string{"hello", "world", "how", "you"}, // "are" is a stop word
		},
		{
			name:     "Numbers and letters",
			input:    "test123 abc456",
			expected: []string{"test123", "abc456"},
		},
		{
			name:     "Case insensitive",
			input:    "HELLO World",
			expected: []string{"hello", "world"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractWords(tt.input)

			// Handle nil vs empty slice comparison
			if len(result) == 0 && len(tt.expected) == 0 {
				return // Both are empty, test passes
			}

			// Sort both slices for comparison
			sort.Strings(result)
			sort.Strings(tt.expected)

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("extractWords() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsEnglishChar(t *testing.T) {
	tests := []struct {
		name     string
		input    rune
		expected bool
	}{
		{"English letter", 'a', true},
		{"English uppercase", 'Z', true},
		{"Digit", '5', true},
		{"Chinese character", '你', false},
		{"Punctuation", '.', false},
		{"Space", ' ', false},
		{"Unicode symbol", '€', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isEnglishChar(tt.input)
			if result != tt.expected {
				t.Errorf("isEnglishChar(%c) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsChineseChar(t *testing.T) {
	tests := []struct {
		name     string
		input    rune
		expected bool
	}{
		{"Simplified Chinese", '你', true},
		{"Traditional Chinese", '風', true},
		{"Chinese punctuation", '，', false}, // Chinese comma is not a Han character
		{"English letter", 'a', false},
		{"Digit", '5', false},
		{"Japanese Hiragana", 'あ', false}, // Not a Han character
		{"Korean Hangul", '안', false},     // Not a Han character
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isChineseChar(tt.input)
			if result != tt.expected {
				t.Errorf("isChineseChar(%c) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNGram_StopWords(t *testing.T) {
	// Test that all expected stop words are filtered
	stopWords := []string{"a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in", "is", "it", "its", "of", "on", "that", "the", "to", "was", "will", "with"}

	for _, stopWord := range stopWords {
		t.Run("Stop word: "+stopWord, func(t *testing.T) {
			result := NGram(stopWord+" hello", 3)

			// Check that the stop word itself is not in the result
			for _, ngram := range result {
				if ngram == stopWord {
					t.Errorf("Stop word '%s' should not appear in n-grams", stopWord)
				}
			}

			// Should still have n-grams from "hello"
			expected := []string{"hel", "ell", "llo"}
			for _, exp := range expected {
				found := false
				for _, ngram := range result {
					if ngram == exp {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected n-gram '%s' not found in result %v", exp, result)
				}
			}
		})
	}
}

func TestNGram_ChineseBigrams(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Simple Chinese phrase",
			input:    "学习编程",
			expected: []string{"学习", "习编", "编程"},
		},
		{
			name:     "Longer Chinese text",
			input:    "我爱学习编程语言",
			expected: []string{"我爱", "爱学", "学习", "习编", "编程", "程语", "语言"},
		},
		{
			name:     "Single Chinese character",
			input:    "中",
			expected: []string{"中"},
		},
		{
			name:     "Two Chinese characters",
			input:    "中国",
			expected: []string{"中国"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NGram(tt.input, 3) // n=3 but Chinese should still use bigrams

			// Sort both slices for comparison
			sort.Strings(result)
			sort.Strings(tt.expected)

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("NGram() for Chinese = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNGram_Performance(t *testing.T) {
	// Test with larger input to ensure performance is reasonable
	largeText := "This is a large text with many words that should be processed efficiently for n-gram generation. " +
		"It contains English words, punctuation marks, and should demonstrate that the n-gram generation " +
		"algorithm can handle substantial amounts of text without performance issues."

	result := NGram(largeText, 3)

	// Should generate many n-grams
	if len(result) < 50 {
		t.Errorf("Expected many n-grams from large text, got %d", len(result))
	}

	// All n-grams should be of length 3 or be short words
	for _, ngram := range result {
		if len([]rune(ngram)) > 3 && isEnglishChar([]rune(ngram)[0]) {
			t.Errorf("Found n-gram longer than 3 characters for English text: %s", ngram)
		}
	}
}
