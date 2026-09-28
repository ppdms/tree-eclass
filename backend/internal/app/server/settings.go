package server

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"tree-eclass/internal/domain/settings"
)

func (s *Server) settingsRoutes() {
	s.mux.HandleFunc("GET /api/notifications/status", s.notificationStatus)
	s.mux.HandleFunc("POST /api/notifications/retry", s.retryNotifications)
	s.mux.HandleFunc("POST /api/v1/settings/credentials", s.saveCredentials)
	s.mux.HandleFunc("POST /api/v1/settings/webhook", s.saveWebhook)
	s.mux.HandleFunc("POST /api/v1/settings/preferences", s.savePreferences)
	s.mux.HandleFunc("GET /api/settings/sync-status", s.syncStatus)
	s.mux.HandleFunc("GET /api/check-status", s.checkStatus)
	s.mux.HandleFunc("POST /api/v1/settings/ai", s.saveAI)
}

func (s *Server) saveAI(w http.ResponseWriter, r *http.Request) {
	form, ok := jsonFields(w, r)
	if !ok {
		return
	}
	value, err := settings.AIFromValues(form)
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
// jsonFields decodes a JSON object with form-shaped keys into url.Values,
// preserving field presence so partial updates behave like form posts.
func jsonFields(w http.ResponseWriter, r *http.Request) (url.Values, bool) {
	var raw map[string]any
	if !bodyJSON(w, r, &raw) {
		return nil, false
	}
	form := make(url.Values, len(raw))
	for key, value := range raw {
		switch v := value.(type) {
		case nil:
			form.Set(key, "")
		case string:
			form.Set(key, v)
		case bool:
			if v {
				form.Set(key, "on")
			} else {
				form.Set(key, "false")
			}
		case float64:
			form.Set(key, strconv.FormatFloat(v, 'f', -1, 64))
		case []any:
			for _, item := range v {
				text, ok := jsonScalar(item)
				if !ok {
					writeFailure(w, http.StatusUnprocessableEntity, "Invalid JSON fields")
					return nil, false
				}
				form.Add(key, text)
			}
		default:
			writeFailure(w, http.StatusUnprocessableEntity, "Invalid JSON fields")
			return nil, false
		}
	}
	return form, true
}
func jsonScalar(value any) (string, bool) {
	switch v := value.(type) {
	case nil:
		return "", true
	case string:
		return v, true
	case bool:
		if v {
			return "on", true
		}
		return "false", true
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	default:
		return "", false
	}
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
	form, ok := jsonFields(w, r)
	if !ok {
		return
	}
	if err := s.settingsService().SaveCredentials(r.Context(), form.Get("username"), form.Get("password"), formBool(form, "clear_password")); err != nil {
		s.settingsError(w, err, 400)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "saved"})
}
func (s *Server) saveWebhook(w http.ResponseWriter, r *http.Request) {
	form, ok := jsonFields(w, r)
	if !ok {
		return
	}
	if err := s.settingsService().SaveWebhook(r.Context(), form.Get("webhook_url"), formBool(form, "clear_webhook")); err != nil {
		s.settingsError(w, err, 400)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "saved"})
}
func (s *Server) savePreferences(w http.ResponseWriter, r *http.Request) {
	form, ok := jsonFields(w, r)
	if !ok {
		return
	}
	if err := s.settingsService().SavePreferences(r.Context(), form); err != nil {
		s.settingsError(w, err, 422)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "saved"})
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
