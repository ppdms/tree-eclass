package server

import (
	"net"
	"net/http"
	"net/url"
	"strings"
)

func localHost(value string) bool {
	host, _, err := net.SplitHostPort(value)
	if err != nil {
		host = value
	}
	host = strings.Trim(strings.ToLower(host), "[]")
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
func (s *Server) trusted(r *http.Request) bool {
	if !s.trustedOrigin(r.Header.Get("Origin")) {
		return false
	}
	if localHost(r.Host) {
		return true
	}
	for _, host := range s.config.AllowedHosts {
		if strings.EqualFold(r.Host, host) {
			return true
		}
	}
	return false
}

func (s *Server) trustedOrigin(origin string) bool {
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return false
	}
	if localHost(parsed.Host) {
		return true
	}
	for _, allowed := range s.config.AllowedOrigins {
		if origin == allowed {
			return true
		}
	}
	return false
}
