package fulltext

func NGram(input string, n int) []string {
	runes := []rune(input)
	if n <= 0 || len(runes) < n {
		return nil
	}

	var ngrams []string
	for i := 0; i <= len(runes)-n; i++ {
		ngrams = append(ngrams, string(runes[i:i+n]))
	}
	return ngrams
}
