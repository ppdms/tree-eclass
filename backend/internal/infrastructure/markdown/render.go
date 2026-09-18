// Package markdown renders model output with raw HTML escaped and unsafe links
// disabled. Bare URLs remain text, matching the established answer renderer.
package markdown

import (
	"bytes"
	"html"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/renderer"
	"github.com/yuin/goldmark/util"
)

type escapedHTML struct{}

func (escapedHTML) RegisterFuncs(r renderer.NodeRendererFuncRegisterer) {
	r.Register(ast.KindRawHTML, escapeHTML)
	r.Register(ast.KindHTMLBlock, escapeHTML)
}
func escapeHTML(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}
	var raw []byte
	switch n := node.(type) {
	case *ast.RawHTML:
		raw = n.Segments.Value(source)
	case *ast.HTMLBlock:
		raw = n.Lines().Value(source)
		if n.HasClosure() {
			raw = bytes.Join([][]byte{raw, n.ClosureLine.Value(source)}, nil)
		}
	}
	_, err := w.WriteString(html.EscapeString(string(raw)))
	return ast.WalkSkipChildren, err
}

var parser = goldmark.New(
	goldmark.WithExtensions(extension.Table, extension.Strikethrough),
	goldmark.WithRendererOptions(renderer.WithNodeRenderers(util.Prioritized(escapedHTML{}, 500))),
)

func Render(text string) string {
	var buffer bytes.Buffer
	text = strings.TrimSpace(text)
	if err := parser.Convert([]byte(text), &buffer); err != nil {
		return "<p>" + html.EscapeString(text) + "</p>"
	}
	return buffer.String()
}
