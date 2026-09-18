package eclass

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoginRenewalAndStreamingDownload(t *testing.T) {
	var logins atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			_ = r.ParseForm()
			if r.Form.Get("uname") != "student" || r.Form.Get("pass") != "secret" {
				t.Error("wrong login form")
			}
			logins.Add(1)
			http.SetCookie(w, &http.Cookie{Name: "PHPSESSID", Value: "synthetic", Path: "/"})
			fmt.Fprint(w, "Welcome")
			return
		}
		if _, err := r.Cookie("PHPSESSID"); err != nil {
			fmt.Fprint(w, `<form action="?login_page=1"><input name="uname"></form>`)
			return
		}
		if r.URL.Path == "/file" {
			if r.Header.Get("If-None-Match") == `"v1"` {
				w.WriteHeader(304)
				return
			}
			w.Header().Set("Content-Type", "application/pdf")
			w.Header().
				Set("Content-Disposition", `attachment; filename="fallback.pdf"; filename*=UTF-8''%CE%94%CE%AD%CE%BD%CE%B4%CF%81%CE%B1.pdf`)
			fmt.Fprint(w, "first")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		fmt.Fprint(w, "Έγγραφα")
	}))
	defer upstream.Close()
	c, _ := New(upstream.URL, "student", "secret")
	data, err := c.Page(t.Context(), "/documents")
	if err != nil || string(data) != "Έγγραφα" || logins.Load() != 1 {
		t.Fatalf("login: %q %v %d", data, err, logins.Load())
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	d, err := c.Download(ctx, "/file", "fallback.pdf", "")
	if err != nil {
		t.Fatal(err)
	}
	if d.Name != "Δένδρα.pdf" {
		t.Fatal(d.Name)
	}
	buf := make([]byte, 5)
	if _, err = io.ReadFull(d.Body, buf); err != nil || string(buf) != "first" {
		t.Fatalf("stream: %q %v", buf, err)
	}
	cancel()
	_ = d.Body.Close()
	d, err = c.Download(t.Context(), "/file", "fallback.pdf", `"v1"`)
	if err != nil || !d.Unchanged || d.Body != nil {
		t.Fatalf("conditional download: %#v %v", d, err)
	}
}

func TestAuthenticationAndRedirectBoundaries(t *testing.T) {
	var contacted atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { contacted.Add(1) }))
	defer other.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/external" {
			http.Redirect(w, r, other.URL+"/private", 302)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "PHPSESSID", Value: "failed", Path: "/"})
		fmt.Fprint(w, `<form action="?login_page=1"><input name="uname"></form>`)
	}))
	defer upstream.Close()
	c, _ := New(upstream.URL, "student", "wrong")
	if _, err := c.Page(t.Context(), "/"); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
	d, err := c.Download(t.Context(), "/external", "Video", "")
	if err != nil || d.Redirect != other.URL+"/private" || contacted.Load() != 0 {
		t.Fatalf("external redirect: %#v %v", d, err)
	}
	if _, err = c.Page(t.Context(), other.URL); err == nil {
		t.Fatal("off-origin authenticated request accepted")
	}
}

func TestPageBoundAndCancellation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/large" {
			fmt.Fprint(w, strings.Repeat("x", maxPage+1))
			return
		}
		<-r.Context().Done()
	}))
	defer upstream.Close()
	c, _ := New(upstream.URL, "student", "secret")
	if _, err := c.Page(t.Context(), "/large"); err == nil {
		t.Fatal("unbounded page accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Page(ctx, "/slow"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
