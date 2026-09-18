package server

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"tree-eclass/internal/domain/settings"
)

func (s *Server) settingsRoutes() {
	s.mux.HandleFunc("GET /api/notifications/status", s.notificationStatus)
	s.mux.HandleFunc("POST /api/notifications/retry", s.retryNotifications)
	s.mux.HandleFunc("POST /settings/credentials", s.saveCredentials)
	s.mux.HandleFunc("POST /settings/webhook", s.saveWebhook)
	s.mux.HandleFunc("POST /settings/preferences", s.savePreferences)
	s.mux.HandleFunc("GET /api/settings/sync-status", s.syncStatus)
	s.mux.HandleFunc("GET /api/check-status", s.checkStatus)
	s.mux.HandleFunc("POST /api/settings/ai", s.saveAI)
}

func (s *Server) saveAI(w http.ResponseWriter, r *http.Request) {
	form, ok := formBody(w, r)
	if !ok {
		return
	}
	value, err := settings.AIFromForm(form)
	if err != nil {
		s.settingsError(w, err, 422)
		return
	}
	if err = s.settingsService().SaveAI(r.Context(), value); err != nil {
		s.settingsError(w, err, 422)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "saved", "chat_provider_order": value.ChatOrder})
}
func (s *Server) settingsService() settings.Service { return settings.Service{Pool: s.db.Pool} }
func formBody(w http.ResponseWriter, r *http.Request) (url.Values, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		err := r.ParseMultipartForm(1024 * 1024)
		if r.MultipartForm != nil {
			defer r.MultipartForm.RemoveAll()
		}
		if err != nil || r.MultipartForm == nil || len(r.MultipartForm.File) > 0 {
			writeFailure(w, http.StatusUnprocessableEntity, "Expected form fields without file attachments")
			return nil, false
		}
		return r.PostForm, true
	}
	if err := r.ParseForm(); err != nil {
		writeFailure(w, http.StatusUnprocessableEntity, "Invalid form")
		return nil, false
	}
	return r.PostForm, true
}
func formBool(form url.Values, key string) bool {
	return form.Get(key) == "on" || form.Get(key) == "true" || form.Get(key) == "1"
}
func (s *Server) settingsError(w http.ResponseWriter, err error, status int) {
	var invalid settings.Invalid
	if errors.As(err, &invalid) {
		writeFailure(w, status, invalid.Message)
		return
	}
	s.internal(w, err)
}
func (s *Server) saveCredentials(w http.ResponseWriter, r *http.Request) {
	form, ok := formBody(w, r)
	if !ok {
		return
	}
	if err := s.settingsService().SaveCredentials(r.Context(), form.Get("username"), form.Get("password"), formBool(form, "clear_password")); err != nil {
		s.settingsError(w, err, 400)
		return
	}
	http.Redirect(w, r, "/settings?credentials_saved=1#credentials", http.StatusSeeOther)
}
func (s *Server) saveWebhook(w http.ResponseWriter, r *http.Request) {
	form, ok := formBody(w, r)
	if !ok {
		return
	}
	if err := s.settingsService().SaveWebhook(r.Context(), form.Get("webhook_url"), formBool(form, "clear_webhook")); err != nil {
		s.settingsError(w, err, 400)
		return
	}
	http.Redirect(w, r, "/settings?webhook_saved=1#webhook", http.StatusSeeOther)
}
func (s *Server) savePreferences(w http.ResponseWriter, r *http.Request) {
	form, ok := formBody(w, r)
	if !ok {
		return
	}
	if err := s.settingsService().SavePreferences(r.Context(), form); err != nil {
		s.settingsError(w, err, 422)
		return
	}
	http.Redirect(w, r, "/settings?preferences_saved=1#preferences", http.StatusSeeOther)
}
func (s *Server) checkStatus(w http.ResponseWriter, r *http.Request) {
	status, err := s.settingsService().Check(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}
func (s *Server) syncStatus(w http.ResponseWriter, r *http.Request) {
	check, err := s.settingsService().Check(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	sync, err := s.settingsService().Sync(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"check": check, "sync": sync})
}
