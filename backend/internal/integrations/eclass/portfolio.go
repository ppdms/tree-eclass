package eclass

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

// PortfolioCourse is one registered course from /main/portfolio.php.
type PortfolioCourse struct {
	ID        int64  `json:"id"`
	Code      string `json:"code"`
	Title     string `json:"title"`
	Professor string `json:"professor"`
}

// infCode matches /courses/INF<digits>/ links; the digits are the local
// course ID used by sync URLs (?course=INF<id>).
var infCode = regexp.MustCompile(`^INF(\d+)$`)

var errNoPortfolio = errors.New("portfolio course table is missing from the eClass response")

// ParsePortfolio extracts registered courses from the portfolio table.
// The portfolio rows carry a TextBold course link, an optional
// parenthesized code, and a professor line. The short parenthesized code
// (3745) is display-only: identity comes from the /courses/INF<id>/ href,
// matching every sync/module URL in this package. Rows without a parseable
// INF link are skipped; an entirely missing table is an error.
func ParsePortfolio(data []byte, base string) ([]PortfolioCourse, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	for _, table := range elements(doc, "table") {
		if attr(table, "id") != "portfolio_lessons" {
			continue
		}
		out := []PortfolioCourse{}
		seen := map[int64]bool{}
		body := first(table, "tbody", "")
		if body == nil {
			body = table
		}
		for _, row := range elements(body, "tr") {
			if c := portfolioRow(row, u); c != nil && !seen[c.ID] {
				seen[c.ID] = true
				out = append(out, *c)
			}
		}
		return out, nil
	}
	return nil, errNoPortfolio
}

func portfolioRow(row *html.Node, base *url.URL) *PortfolioCourse {
	link := first(row, "a", "TextBold")
	if link == nil {
		return nil
	}
	target, err := base.Parse(attr(link, "href"))
	if err != nil || target.Host != base.Host || target.Scheme != base.Scheme {
		return nil
	}
	parts := strings.FieldsFunc(
		strings.Trim(target.Path, "/"),
		func(r rune) bool { return r == '/' },
	)
	if len(parts) != 2 || parts[0] != "courses" {
		return nil
	}
	match := infCode.FindStringSubmatch(parts[1])
	if match == nil {
		return nil
	}
	id, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil || id < 1 {
		return nil
	}
	cells := elements(row, "td")
	professor := ""
	if len(cells) > 0 {
		for _, small := range elements(cells[0], "small") {
			if class(small, "vsmall-text") {
				professor = strings.TrimSpace(nodeText(small))
				break
			}
		}
	}
	return &PortfolioCourse{
		ID:        id,
		Code:      parts[1],
		Title:     strings.TrimSpace(nodeText(link)),
		Professor: professor,
	}
}

// Portfolio fetches the registered courses. countPages=-1 disables the
// client-side DataTables pagination server-side, so one fetch returns all
// rows; the unpaginated fallback merges both portfolio URLs by course ID in
// case a future upstream theme serves a subset on either.
func (c *Client) Portfolio(ctx context.Context) ([]PortfolioCourse, error) {
	merged := []PortfolioCourse{}
	seen := map[int64]bool{}
	appendPage := func(raw string) (bool, error) {
		data, err := c.Page(ctx, raw)
		if err != nil {
			return false, err
		}
		page, err := ParsePortfolio(data, c.base.String())
		if err != nil {
			return false, err
		}
		changed := false
		for _, course := range page {
			if !seen[course.ID] {
				seen[course.ID] = true
				merged = append(merged, course)
				changed = true
			}
		}
		return changed, nil
	}
	if _, err := appendPage("/main/portfolio.php?countPages=-1"); err == nil {
		return merged, nil
	}
	var listAllErr error
	for _, raw := range []string{"/main/portfolio.php", "/main/portfolio.php?countPages=-1"} {
		if _, err := appendPage(raw); err != nil {
			if listAllErr == nil {
				listAllErr = err
			}
			continue
		}
	}
	if len(merged) > 0 {
		return merged, nil
	}
	return nil, listAllErr
}
