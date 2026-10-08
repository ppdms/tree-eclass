package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"tree-eclass/internal/domain/courses"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/integrations/eclass"
)

// WithEclassBaseURL pins the upstream origin for the portfolio catalog.
// Production callers leave it empty so eclass.BaseURL is used; tests point
// it at a synthetic httptest server. Runtime configuration cannot override
// endpoints.
func WithEclassBaseURL(raw string) Option {
	return func(s *Server) { s.eclassBaseURL = raw }
}

func (s *Server) eclassBase() string {
	if s.eclassBaseURL != "" {
		return s.eclassBaseURL
	}
	return eclass.BaseURL
}

// availableCourse is one registered upstream course plus local state, so the
// Add-course picker can preselect known names and hide the already-tracked.
type availableCourse struct {
	eclass.PortfolioCourse
	Registered bool   `json:"registered"`
	Name       string `json:"name"`
}

func (s *Server) availableCourses(w http.ResponseWriter, r *http.Request) {
	if !s.availableMu.TryLock() {
		writeFailure(w, http.StatusConflict, "The registered-course catalog is already loading.")
		return
	}
	defer s.availableMu.Unlock()
	portfolio, err := s.crawlPortfolio(r)
	if err != nil {
		s.eclassFailure(w, err)
		return
	}
	registered, err := s.courseService().List(r.Context(), true)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"courses": mergeAvailable(portfolio, registered)})
}

func (s *Server) crawlPortfolio(r *http.Request) ([]eclass.PortfolioCourse, error) {
	credentials, err := s.settingsService().Credentials(r.Context())
	if err != nil {
		return nil, err
	}
	if credentials == nil || credentials.Password == "" {
		return nil, errMissingCredentials
	}
	prefs, err := s.settingsService().Preferences(r.Context())
	if err != nil {
		return nil, err
	}
	source, err := eclass.New(s.eclassBase(), credentials.Username, credentials.Password)
	if err != nil {
		return nil, err
	}
	defer source.Close()
	source.SetTimeout(time.Duration(prefs.Timeout) * time.Second)
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	if err = source.Login(ctx); err != nil {
		return nil, err
	}
	return source.Portfolio(ctx)
}

var errMissingCredentials = errors.New("Save eClass credentials before loading registered courses.")

func mergeAvailable(portfolio []eclass.PortfolioCourse, registered []courses.Course) []availableCourse {
	known := map[int64]string{}
	for _, course := range registered {
		known[course.ID] = course.Name
	}
	out := make([]availableCourse, 0, len(portfolio))
	for _, course := range portfolio {
		item := availableCourse{PortfolioCourse: course, Name: course.Title}
		if name, ok := known[course.ID]; ok {
			item.Registered, item.Name = true, name
		}
		out = append(out, item)
	}
	return out
}
func (s *Server) eclassFailure(w http.ResponseWriter, err error) {
	if errors.Is(err, errMissingCredentials) {
		writeFailure(w, http.StatusConflict, errMissingCredentials.Error())
		return
	}
	if errors.Is(err, eclass.ErrAuthentication) {
		writeFailure(w, http.StatusBadGateway, "eClass rejected the saved credentials.")
		return
	}
	if errors.Is(err, eclass.ErrRegistration) {
		writeFailure(w, http.StatusBadGateway, "eClass requires course registration before reading it.")
		return
	}
	var invalid settings.Invalid
	if errors.As(err, &invalid) {
		writeFailure(w, http.StatusUnprocessableEntity, invalid.Message)
		return
	}
	s.internal(w, err)
}
