package eclass

import (
	"bytes"
	"strings"

	"golang.org/x/net/html"
)

func attr(n *html.Node, key string) string {
	if n == nil {
		return ""
	}
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}
func class(n *html.Node, value string) bool {
	for _, s := range strings.Fields(attr(n, "class")) {
		if s == value {
			return true
		}
	}
	return false
}
func elements(root *html.Node, tag string) []*html.Node {
	result := []*html.Node{}
	if root == nil {
		return result
	}
	for n := range root.Descendants() {
		if n.Type == html.ElementNode && n.Data == tag {
			result = append(result, n)
		}
	}
	return result
}
func first(root *html.Node, tag, cssClass string) *html.Node {
	if root == nil {
		return nil
	}
	for n := range root.Descendants() {
		if n.Type == html.ElementNode && n.Data == tag && (cssClass == "" || class(n, cssClass)) {
			return n
		}
	}
	return nil
}
func nodeText(n *html.Node) string {
	if n == nil {
		return ""
	}
	var out strings.Builder
	for child := range n.Descendants() {
		if child.Type == html.TextNode {
			out.WriteString(strings.TrimSpace(child.Data))
		}
	}
	return out.String()
}
func firstText(n *html.Node, fallback bool) string {
	if n == nil {
		return ""
	}
	for child := range n.ChildNodes() {
		if child.Type == html.ElementNode {
			break
		}
		if child.Type == html.TextNode && strings.TrimSpace(child.Data) != "" {
			return strings.TrimSpace(child.Data)
		}
	}
	if fallback {
		return nodeText(n)
	}
	return ""
}
func innerHTML(n *html.Node) string {
	if n == nil {
		return ""
	}
	var b bytes.Buffer
	for child := range n.ChildNodes() {
		_ = html.Render(&b, child)
	}
	return strings.TrimSpace(b.String())
}
