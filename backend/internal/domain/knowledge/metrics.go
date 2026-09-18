package knowledge

import (
	"math"
	"regexp"
	"unicode/utf8"
)

var metricWords = regexp.MustCompile(`[\p{L}\p{N}]+(?:[-'’][\p{L}\p{N}]+)*`)
var metricSentences = regexp.MustCompile(`[.!?;·…]+(?:[\s\p{Z}\x0b]|$)`)
var metricSymbols = regexp.MustCompile(`[{}\[\]()=<>/*+\-^%|]`)

type Metrics struct {
	Characters      int64  `json:"character_count"`
	Words           int64  `json:"word_count"`
	ReadingMinutes  int64  `json:"reading_minutes"`
	ComplexityScore int64  `json:"complexity_score"`
	ComplexityLabel string `json:"complexity_label"`
}
type metricAccumulator struct {
	characters, words, wordCharacters, longWords, sentences, symbols int64
	units                                                            int
}

func (m *metricAccumulator) Add(text string) {
	if text == "" {
		return
	}
	if m.units > 0 {
		m.characters++
	}
	m.units++
	m.characters += int64(utf8.RuneCountInString(text))
	for _, word := range metricWords.FindAllString(text, -1) {
		n := int64(utf8.RuneCountInString(word))
		m.words++
		m.wordCharacters += n
		if n >= 9 {
			m.longWords++
		}
	}
	m.sentences += int64(len(metricSentences.FindAllStringIndex(text, -1)))
	m.symbols += int64(len(metricSymbols.FindAllStringIndex(text, -1)))
}
func (m metricAccumulator) Result() Metrics {
	score := int64(0)
	clamp := func(v float64) float64 { return min(1, max(0, v)) }
	if m.words > 0 {
		words := float64(m.words)
		value := 0.38*clamp(
			words/float64(max(1, m.sentences))/32,
		) + 0.24*clamp(
			(float64(m.wordCharacters)/words-3.5)/4.5,
		) + 0.23*clamp(
			float64(m.longWords)/words/0.24,
		) + 0.15*clamp(
			float64(m.symbols)/float64(max(1, m.characters))/0.07,
		)
		score = int64(math.RoundToEven(100 * value))
	}
	label := "Very dense"
	switch {
	case score < 30:
		label = "Accessible"
	case score < 55:
		label = "Moderate"
	case score < 75:
		label = "Dense"
	}
	return Metrics{m.characters, m.words, (m.words + 219) / 220, score, label}
}
