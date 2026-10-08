package workflow

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"tree-eclass/internal/app/server"
	"tree-eclass/internal/domain/identity"
)

func TestNativeAvailableCatalogListsPortfolio(t *testing.T) {
	c := nativeSharedController(t)
	ctx := context.Background()
	conn, _ := startTestStorage(t, c)
	t.Cleanup(func() { conn.Close(ctx) })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/main/login_form.php":
			http.SetCookie(w, &http.Cookie{Name: "PHPSESSID", Value: "catalog-session", Path: "/"})
			fmt.Fprint(w, `<form><input name="uname"></form>`)
		case r.Method == http.MethodPost:
			http.SetCookie(w, &http.Cookie{Name: "PHPSESSID", Value: "catalog-session", Path: "/"})
			fmt.Fprint(w, "Welcome")
		default:
			fmt.Fprint(w, `<table id="portfolio_lessons"><tbody>`+
				`<tr><td><div><a class="TextBold" href="http://`+r.Host+`/courses/INF543/">Catalog title</a></div>`+
				`<div><small class="vsmall-text">Catalog professor</small></div></td></tr>`+
				`</tbody></table>`)
		}
	}))
	t.Cleanup(upstream.Close)
	if _, err := conn.Exec(
		ctx,
		`INSERT INTO app.credentials(id,username,password) VALUES(1,$1,$2)`,
		identity.Encode("synthetic student"),
		identity.Encode("synthetic-password"),
	); err != nil {
		t.Fatal(err)
	}
	api, err := server.New(
		ctx,
		server.Config{DatabaseURL: c.databaseURL(), ObjectsRoot: c.testObjectsRoot(), Mode: "test"},
		server.WithEclassBaseURL(upstream.URL),
	)
	if err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(api)
	t.Cleanup(func() {
		host.Close()
		api.Close()
	})
	var catalog struct {
		Courses []struct {
			ID         int64  `json:"id"`
			Code       string `json:"code"`
			Title      string `json:"title"`
			Professor  string `json:"professor"`
			Name       string `json:"name"`
			Registered bool   `json:"registered"`
		} `json:"courses"`
	}
	apiJSON(t, "GET", host.URL+"/api/v1/courses/available", nil, 200, &catalog)
	if len(catalog.Courses) != 1 || catalog.Courses[0].ID != 543 || catalog.Courses[0].Name != "Catalog title" {
		t.Fatalf("portfolio catalog: %#v", catalog)
	}
	apiJSON(
		t,
		"POST",
		host.URL+"/api/v1/courses",
		map[string]any{"course_id": 543, "name": "Edited name", "short_name": "KT"},
		200,
		nil,
	)
	apiJSON(t, "GET", host.URL+"/api/v1/courses/available", nil, 200, &catalog)
	if len(catalog.Courses) != 1 || !catalog.Courses[0].Registered || catalog.Courses[0].Name != "Edited name" {
		t.Fatalf("catalog did not reflect local edit: %#v", catalog)
	}
	var short *string
	if err = conn.QueryRow(ctx, `SELECT short_name FROM app.courses WHERE id=543`).Scan(&short); err != nil ||
		short == nil || identity.Decode(*short) != "KT" {
		t.Fatalf("short name not stored: %#v %v", short, err)
	}
}
