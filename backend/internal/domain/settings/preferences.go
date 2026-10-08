// Package settings owns durable configuration and explicit secret replacement.
package settings

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Service struct{ Pool database.Store }
type Preferences struct {
	CheckInterval  int64   `json:"check_interval_minutes"`
	Downloads      int64   `json:"max_concurrent_downloads"`
	Timeout        int64   `json:"request_timeout_seconds"`
	Retries        int64   `json:"retry_attempts"`
	Notifications  bool    `json:"notification_enabled"`
	NotifyErrors   bool    `json:"notification_on_error"`
	DepartmentFeed bool    `json:"global_feed_dept_enabled"`
	UndergradFeed  bool    `json:"global_feed_undergrad_enabled"`
	RectorFeed     bool    `json:"global_feed_rector_enabled"`
	SemesterStart  *string `json:"semester_start"`
	SemesterEnd    *string `json:"semester_end"`
	BasePath       string  `json:"download_base_path"`
}

func readPreferences(ctx context.Context, db database.Operations) (Preferences, error) {
	p := Preferences{
		CheckInterval:  60,
		Downloads:      3,
		Timeout:        30,
		Retries:        3,
		Notifications:  true,
		NotifyErrors:   true,
		DepartmentFeed: true,
		BasePath:       "/University",
	}
	stored, err := db.Settings().LoadPreferences(ctx)
	if database.IsNoRows(err) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	p.CheckInterval = stored.CheckInterval
	p.Downloads = stored.Downloads
	p.Timeout = stored.Timeout
	p.Retries = stored.Retries
	p.Notifications = stored.Notifications
	p.NotifyErrors = stored.NotifyErrors
	p.DepartmentFeed = stored.DepartmentFeed
	p.UndergradFeed = stored.UndergradFeed
	p.RectorFeed = stored.RectorFeed
	p.SemesterStart = stored.SemesterStart
	p.SemesterEnd = stored.SemesterEnd
	p.BasePath = normalizeBasePath(stored.BasePath)
	return p, nil
}
func (s Service) Preferences(ctx context.Context) (Preferences, error) {
	return readPreferences(ctx, s.Pool)
}
func (p *Preferences) Apply(form url.Values) error {
	for _, field := range []struct {
		name     string
		target   *int64
		min, max int64
	}{
		{"check_interval_minutes", &p.CheckInterval, 5, 1440}, {"max_concurrent_downloads", &p.Downloads, 1, 10}, {"request_timeout_seconds", &p.Timeout, 10, 300}, {"retry_attempts", &p.Retries, 0, 10},
	} {
		if !form.Has(field.name) {
			continue
		}
		value, err := strconv.ParseInt(strings.TrimSpace(form.Get(field.name)), 10, 64)
		if err != nil || value < field.min || value > field.max {
			return fmt.Errorf("%s must be a whole number between %d and %d", field.name, field.min, field.max)
		}
		*field.target = value
	}
	for name, target := range map[string]*bool{
		"notification_enabled":          &p.Notifications,
		"notification_on_error":         &p.NotifyErrors,
		"global_feed_dept_enabled":      &p.DepartmentFeed,
		"global_feed_undergrad_enabled": &p.UndergradFeed,
		"global_feed_rector_enabled":    &p.RectorFeed,
	} {
		if form.Has(name) {
			*target = form.Get(name) == "on"
		}
	}
	for name, target := range map[string]**string{"semester_start": &p.SemesterStart, "semester_end": &p.SemesterEnd} {
		if form.Has(name) {
			text := form.Get(name)
			*target = &text
		}
	}
	if form.Has("download_base_path") {
		value := strings.TrimSpace(form.Get("download_base_path"))
		if value == "" {
			p.BasePath = ""
		} else {
			p.BasePath = identity.Path(value)
			if p.BasePath == "/" {
				return errors.New("download base path must be a non-root local path")
			}
			for _, part := range strings.Split(strings.TrimPrefix(p.BasePath, "/"), "/") {
				if part == "." || part == ".." {
					return errors.New("download base path must not contain dot segments")
				}
			}
		}
	}
	return nil
}

func normalizeBasePath(value string) string {
	value = strings.TrimSpace(identity.Decode(value))
	if value == "" {
		return ""
	}
	return identity.Path(value)
}
func (s Service) SavePreferences(ctx context.Context, form url.Values) error {
	return s.mutate(ctx, "preferences", func(tx database.Tx) error {
		p, err := readPreferences(ctx, tx)
		if err != nil {
			return err
		}
		if err = p.Apply(form); err != nil {
			return Invalid{err.Error()}
		}
		return tx.Settings().SavePreferences(ctx, database.SettingsPreferences{
			CheckInterval:  p.CheckInterval,
			Downloads:      p.Downloads,
			Timeout:        p.Timeout,
			Retries:        p.Retries,
			Notifications:  p.Notifications,
			NotifyErrors:   p.NotifyErrors,
			DepartmentFeed: p.DepartmentFeed,
			UndergradFeed:  p.UndergradFeed,
			RectorFeed:     p.RectorFeed,
			SemesterStart:  p.SemesterStart,
			SemesterEnd:    p.SemesterEnd,
			BasePath:       identity.Encode(p.BasePath),
		})
	})
}

type Invalid struct{ Message string }

func (e Invalid) Error() string { return e.Message }
func (s Service) mutate(ctx context.Context, key string, fn func(database.Tx) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = tx.Settings().LockSettingsSection(ctx, key); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
