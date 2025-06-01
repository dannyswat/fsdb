package fulltext

import (
	"strings"
	"unicode"
)

// Common English stop words
var englishStopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "by": true, "for": true, "from": true, "has": true, "he": true,
	"in": true, "is": true, "it": true, "its": true, "of": true, "on": true,
	"that": true, "the": true, "to": true, "was": true, "will": true, "with": true,
}

// isEnglishChar checks if a character is ASCII/English
func isEnglishChar(r rune) bool {
	return r <= 127 && (unicode.IsLetter(r) || unicode.IsDigit(r))
}

// isChineseChar checks if a character is Chinese
func isChineseChar(r rune) bool {
	return unicode.Is(unicode.Han, r)
}

// extractWords extracts words from input, separating English and Unicode text
func extractWords(input string) []string {
	runes := []rune(strings.ToLower(input))
	var words []string = make([]string, 0)
	var currentWord []rune
	var isCurrentWordEnglish bool

	for _, r := range runes {
		if unicode.IsSpace(r) || unicode.IsPunct(r) {
			// End current word
			if len(currentWord) > 0 {
				word := string(currentWord)
				// Skip English stop words
				if !isCurrentWordEnglish || !englishStopWords[word] {
					words = append(words, word)
				}
				currentWord = nil
			}
			continue
		}

		isEnglish := isEnglishChar(r)
		isChinese := isChineseChar(r)

		// Skip if neither English nor Chinese
		if !isEnglish && !isChinese {
			continue
		}

		// If starting a new word or language changes, finalize current word
		if len(currentWord) == 0 {
			isCurrentWordEnglish = isEnglish
			currentWord = append(currentWord, r)
		} else if (isCurrentWordEnglish && !isEnglish) || (!isCurrentWordEnglish && !isChinese) {
			// Language boundary - finalize current word
			word := string(currentWord)
			if !isCurrentWordEnglish || !englishStopWords[word] {
				words = append(words, word)
			}
			currentWord = []rune{r}
			isCurrentWordEnglish = isEnglish
		} else {
			currentWord = append(currentWord, r)
		}
	}

	// Add final word
	if len(currentWord) > 0 {
		word := string(currentWord)
		if !isCurrentWordEnglish || !englishStopWords[word] {
			words = append(words, word)
		}
	}

	return words
}

func NGram(input string, n int) []string {
	if n <= 0 {
		return nil
	}

	words := extractWords(input)
	var allNgrams []string

	for _, word := range words {
		runes := []rune(word)

		// For Chinese text, always use bigrams (n=2)
		if isChineseChar(runes[0]) {
			if len(runes) >= 2 {
				for i := 0; i <= len(runes)-2; i++ {
					allNgrams = append(allNgrams, string(runes[i:i+2]))
				}
			} else {
				// Single Chinese character
				allNgrams = append(allNgrams, word)
			}
			continue
		}

		// For English words, treat the whole word as a unit if shorter than n
		if isEnglishChar(runes[0]) && len(runes) <= n {
			allNgrams = append(allNgrams, word)
			continue
		}

		// Generate n-grams for this word
		if len(runes) >= n {
			for i := 0; i <= len(runes)-n; i++ {
				allNgrams = append(allNgrams, string(runes[i:i+n]))
			}
		}
	}

	return allNgrams
}
