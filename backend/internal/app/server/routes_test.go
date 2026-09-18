package server

import (
	"net/http"
	"testing"
)

func TestRoutesAssemble(t *testing.T) {
	s := &Server{mux: http.NewServeMux()}
	s.routes()
}
