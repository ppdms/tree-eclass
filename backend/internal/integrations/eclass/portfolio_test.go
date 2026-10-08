package eclass

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const portfolioRowTemplate = `<tr class="row-course"><td>` +
	`<div><a class="TextBold" href="%s/courses/%s/">%s</a><small>(%s)</small></div>` +
	`<div><small class="vsmall-text Neutral-900-cl TextRegular">%s</small></div></td></tr>`

func portfolioFixture(rows string) string {
	return `<table id="portfolio_lessons"><tbody>` + rows + `</tbody></table>`
}

func TestPortfolioParsesTitlesAndProfessors(t *testing.T) {
	rows := fmt.Sprintf(
		portfolioRowTemplate,
		BaseURL,
		"INF543",
		"ΚΑΤΑΝΕΜΗΜΕΝΑ ΣΥΣΤΗΜΑΤΑ ΕΑΡΙΝΟ 2026",
		"INF543",
		"ΒΑΣΙΛΙΚΗ ΚΑΛΟΓΕΡΑΚΗ",
	)
	// The parenthesized code is display-only: identity stays on the INF link.
	rows += fmt.Sprintf(
		portfolioRowTemplate,
		BaseURL,
		"INF267",
		"Μηχανική Μάθηση (Προπτυχιακό)",
		"3745",
		"ΘΕΜΟΣ ΣΤΑΦΥΛΑΚΗΣ",
	)
	rows += fmt.Sprintf(portfolioRowTemplate, BaseURL, "INF267", "Duplicate row", "3745", "Other")
	// Rows without an INF course link (collaborations, off-site) are skipped.
	rows += `<tr><td><a class="TextBold" href="/courses/COLL1/">Collaboration</a></td></tr>`
	rows += `<tr><td><a class="TextBold" href="https://unrelated.invalid/courses/INF999/">Off-site</a></td></tr>`
	courses, err := ParsePortfolio([]byte(portfolioFixture(rows)), BaseURL)
	if err != nil || len(courses) != 2 {
		t.Fatalf("portfolio: %#v %v", courses, err)
	}
	first := courses[0]
	if first.ID != 543 || first.Code != "INF543" || first.Professor != "ΒΑΣΙΛΙΚΗ ΚΑΛΟΓΕΡΑΚΗ" {
		t.Fatal("first course identity lost", first)
	}
	second := courses[1]
	if second.ID != 267 || second.Title != "Μηχανική Μάθηση (Προπτυχιακό)" {
		t.Fatal("display-only code overrode INF identity", second)
	}
	if _, err = ParsePortfolio([]byte(`<h1>No table here</h1>`), BaseURL); err == nil {
		t.Fatal("missing portfolio table accepted")
	}
}

func TestPortfolioFallsBackToUnpaginatedPage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		row := func(code, title string) string {
			return fmt.Sprintf(portfolioRowTemplate, "http://"+r.Host, code, title, code, "Prof")
		}
		switch {
		case r.URL.Path == "/main/login_form.php":
			http.SetCookie(w, &http.Cookie{Name: "PHPSESSID", Value: "portfolio-session", Path: "/"})
			fmt.Fprint(w, `<form><input name="uname"></form>`)
		case strings.Contains(r.URL.RawQuery, "countPages=-1"):
			http.Error(w, "legacy theme without list-all", http.StatusNotFound)
		default:
			fmt.Fprint(w, portfolioFixture(row("INF543", "First page")))
		}
	}))
	defer upstream.Close()
	client, err := New(upstream.URL, "student", "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err = client.Login(t.Context()); err != nil {
		t.Fatal(err)
	}
	courses, err := client.Portfolio(t.Context())
	if err != nil || len(courses) != 1 || courses[0].ID != 543 {
		t.Fatalf("unpaginated portfolio fallback: %#v %v", courses, err)
	}
}
