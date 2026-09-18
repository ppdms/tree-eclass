// Package identity preserves source-based identifiers and Greek text semantics.
package identity

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

func Path(value string) string {
	parts := strings.FieldsFunc(strings.TrimSpace(norm.NFC.String(value)), func(r rune) bool { return r == '/' })
	return "/" + strings.Join(parts, "/")
}
func Stable(prefix string, parts ...string) string {
	hash := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + "_" + fmt.Sprintf("%x", hash)[:32]
}
func Document(courseID int64, path string) string {
	return Stable("doc", fmt.Sprint(courseID), Path(path))
}
func Search(text string) string {
	decomposed := norm.NFD.String(cases.Fold().String(norm.NFC.String(text)))
	folded := strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Mn, r) {
			return -1
		}
		return r
	}, decomposed)
	return strings.Join(strings.Fields(norm.NFC.String(folded)), " ")
}
func TextHash(text string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(norm.NFC.String(text))))
}
func Encode(text string) string {
	return strings.ReplaceAll(strings.ReplaceAll(text, "\ue000", "\ue000e"), "\x00", "\ue0000")
}
func Decode(text string) string {
	return strings.NewReplacer("\ue0000", "\x00", "\ue000e", "\ue000").Replace(text)
}
