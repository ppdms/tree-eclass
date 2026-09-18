package server

import (
	"net/http/httptest"
	"testing"
)

func TestNativeTrustBoundary(t *testing.T) {
	s := &Server{}
	for _, test := range []struct {
		host, origin string
		allowed      bool
	}{
		{"127.0.0.1:8001", "http://localhost:8000", true},
		{"localhost:8001", "", true},
		{"127.0.0.1:8001", "https://untrusted.example", false},
		{"rebind.example:8001", "", false},
		{"127.0.0.1:8001", "null", false},
		{"127.0.0.1:8001", "https://localhost.untrusted.example", false},
	} {
		r := httptest.NewRequest("POST", "http://"+test.host+"/courses/add", nil)
		r.Header.Set("Origin", test.origin)
		if s.trusted(r) != test.allowed {
			t.Errorf("host=%q origin=%q", test.host, test.origin)
		}
	}
}
