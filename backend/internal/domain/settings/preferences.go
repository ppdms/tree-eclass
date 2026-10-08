// Package settings owns durable configuration and explicit secret replacement.
package settings

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Service struct{ Pool rdbms.Pool }
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
type queryer interface {
	QueryRow(context.Context, string, ...any) rdbms.Row
}

func readPreferences(ctx context.Context, db queryer) (Preferences, error) {
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
	err := db.QueryRow(ctx, `SELECT check_interval_minutes,max_concurrent_downloads,request_timeout_seconds,retry_attempts,notification_enabled=1,notification_on_error=1,global_feed_dept_enabled=1,global_feed_undergrad_enabled=1,global_feed_rector_enabled=1,semester_start,semester_end,download_base_path FROM app.preferences WHERE id=1`).
		Scan(
			&p.CheckInterval,
			&p.Downloads,
			&p.Timeout,
			&p.Retries,
			&p.Notifications,
			&p.NotifyErrors,
			&p.DepartmentFeed,
			&p.UndergradFeed,
			&p.RectorFeed,
			&p.SemesterStart,
			&p.SemesterEnd,
			&p.BasePath,
		)
	if errors.Is(err, rdbms.ErrNoRows) {
		err = nil
	}
	p.BasePath = normalizeBasePath(p.BasePath)
	return p, err
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
func flag(value bool) int {
	if value {
		return 1
	}
	return 0
}
func (s Service) SavePreferences(ctx context.Context, form url.Values) error {
	return s.mutate(ctx, "preferences", func(tx rdbms.Tx) error {
		p, err := readPreferences(ctx, tx)
		if err != nil {
			return err
		}
		if err = p.Apply(form); err != nil {
			return Invalid{err.Error()}
		}
		_, err = tx.Exec(
			ctx,
			`INSERT INTO app.preferences(id,check_interval_minutes,max_concurrent_downloads,request_timeout_seconds,retry_attempts,notification_enabled,notification_on_error,global_feed_dept_enabled,global_feed_undergrad_enabled,global_feed_rector_enabled,semester_start,semester_end,download_base_path)
 VALUES(1,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(id) DO UPDATE SET check_interval_minutes=$1,max_concurrent_downloads=$2,request_timeout_seconds=$3,retry_attempts=$4,notification_enabled=$5,notification_on_error=$6,global_feed_dept_enabled=$7,global_feed_undergrad_enabled=$8,global_feed_rector_enabled=$9,semester_start=$10,semester_end=$11,download_base_path=$12`,
			p.CheckInterval,
			p.Downloads,
			p.Timeout,
			p.Retries,
			flag(p.Notifications),
			flag(p.NotifyErrors),
			flag(p.DepartmentFeed),
			flag(p.UndergradFeed),
			flag(p.RectorFeed),
			p.SemesterStart,
			p.SemesterEnd,
			identity.Encode(p.BasePath),
		)
		return err
	})
}

type Invalid struct{ Message string }

func (e Invalid) Error() string { return e.Message }
func (s Service) mutate(ctx context.Context, key string, fn func(rdbms.Tx) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('settings:'||$1,0))`, key); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
