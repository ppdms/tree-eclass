package markdown

import (
	"strings"
	"testing"
)

func TestUntrustedMarkdown(t *testing.T) {
	html := Render(
		"# Answer\n\n**Bold** and ~~old~~.\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n<script>alert('bad')</script>\n\n[unsafe](javascript:alert(1))\n\nhttps://example.invalid",
	)
	for _, expected := range []string{"<h1>Answer</h1>", "<strong>Bold</strong>", "<del>old</del>", "<table>", "&lt;script&gt;"} {
		if !strings.Contains(html, expected) {
			t.Errorf("missing %q from %s", expected, html)
		}
	}
	for _, unsafe := range []string{"<script>", `href="javascript:`, `href="https://example.invalid"`} {
		if strings.Contains(html, unsafe) {
			t.Errorf("unsafe markup %q", unsafe)
		}
	}
}
