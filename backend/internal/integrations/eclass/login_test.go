package eclass

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestLoginEstablishesSessionBeforeSubmittingCredentials(t *testing.T) {
	var submissions atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/main/login_form.php" {
			http.SetCookie(w, &http.Cookie{Name: "PHPSESSID", Value: "session-before-login", Path: "/"})
			fmt.Fprint(
				w,
				`<form action="/?login_page=1"><input name="uname"><input name="pass" type="password"></form>`,
			)
			return
		}
		submissions.Add(1)
		cookie, err := r.Cookie("PHPSESSID")
		if err != nil || cookie.Value != "session-before-login" {
			http.Error(w, "Cookies must be enabled before login", http.StatusForbidden)
			return
		}
		if err := r.ParseForm(); err != nil || r.Form.Get("uname") != "student" || r.Form.Get("pass") != "secret" {
			http.Error(w, "Invalid login form", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, "Authenticated portfolio")
	}))
	defer upstream.Close()
	client, err := New(upstream.URL, "student", "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Login(t.Context()); err != nil {
		t.Fatal(err)
	}
	if submissions.Load() != 1 {
		t.Fatal("expected one authenticated form submission", submissions.Load())
	}
}

func TestLoginDoesNotSubmitCredentialsAfterFailedSessionSetup(t *testing.T) {
	var submissions atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			submissions.Add(1)
		}
		http.Error(w, "Unavailable", http.StatusServiceUnavailable)
	}))
	defer upstream.Close()
	client, _ := New(upstream.URL, "student", "secret")
	defer client.Close()
	if err := client.Login(t.Context()); err == nil || submissions.Load() != 0 {
		t.Fatal("credentials submitted without a working login page", err, submissions.Load())
	}
}
