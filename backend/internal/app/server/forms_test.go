package server

import (
	"bytes"
	"mime/multipart"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFormBodyAcceptsBrowserMultipartAndExcludesQueryFields(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("username", "synthetic-student")
	_ = writer.WriteField("password", "synthetic-password")
	_ = writer.Close()
	req := httptest.NewRequest("POST", "/settings/credentials?username=wrong&clear_password=on", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	form, ok := formBody(httptest.NewRecorder(), req)
	if !ok || form.Get("username") != "synthetic-student" || form.Get("password") != "synthetic-password" ||
		form.Has("clear_password") {
		t.Fatal("multipart form boundary failed")
	}
}

func TestFormBodyRejectsFilesAndOversizedFields(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, _ := writer.CreateFormFile("token", "attachment.txt")
	_, _ = file.Write([]byte("unexpected"))
	_ = writer.Close()
	req := httptest.NewRequest("POST", "/settings/discord-exporter", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if _, ok := formBody(httptest.NewRecorder(), req); ok {
		t.Fatal("unexpected file attachment accepted")
	}
	req = httptest.NewRequest(
		"POST",
		"/settings/credentials",
		strings.NewReader("username="+strings.Repeat("x", 1024*1024)),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if _, ok := formBody(httptest.NewRecorder(), req); ok {
		t.Fatal("unbounded form accepted")
	}
}
