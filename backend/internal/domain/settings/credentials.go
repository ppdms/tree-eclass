package settings

import (
	"context"
	"errors"
	"strings"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/rdbms"
)

type Credentials struct {
	Username string `json:"username"`
	Password string `json:"-"`
}

func readCredentials(ctx context.Context, db queryer) (*Credentials, error) {
	var result Credentials
	err := db.QueryRow(ctx, `SELECT username,password FROM app.credentials WHERE id=1`).
		Scan(&result.Username, &result.Password)
	if errors.Is(err, rdbms.ErrNoRows) {
		return nil, nil
	}
	result.Username, result.Password = identity.Decode(result.Username), identity.Decode(result.Password)
	return &result, err
}
func (s Service) Credentials(ctx context.Context) (*Credentials, error) {
	return readCredentials(ctx, s.Pool)
}
func (s Service) SaveCredentials(ctx context.Context, username, password string, clear bool) error {
	username = strings.TrimSpace(username)
	if username == "" {
		return Invalid{"Username is required"}
	}
	return s.mutate(ctx, "credentials", func(tx rdbms.Tx) error {
		existing, err := readCredentials(ctx, tx)
		if err != nil {
			return err
		}
		final := password
		switch {
		case clear:
			final = ""
		case strings.TrimSpace(password) != "":
		case existing != nil && existing.Username != username:
			return Invalid{"Enter the password again when changing username"}
		case existing != nil:
			final = existing.Password
		default:
			final = ""
		}
		_, err = tx.Exec(
			ctx,
			`INSERT INTO app.credentials(id,username,password) VALUES(1,$1,$2) ON CONFLICT(id) DO UPDATE SET username=$1,password=$2`,
			identity.Encode(username),
			identity.Encode(final),
		)
		if err != nil {
			return err
		}
		// Saved cookies must not authenticate the previous credential pair.
		if existing == nil || existing.Username != username || existing.Password != final {
			_, err = tx.Exec(ctx, `DELETE FROM app.app_data WHERE key='session_cookie'`)
		}
		return err
	})
}
func (s Service) Webhook(ctx context.Context) (string, error) {
	var value string
	err := s.Pool.QueryRow(ctx, `SELECT webhook_url FROM app.webhook_config WHERE id=1`).Scan(&value)
	if errors.Is(err, rdbms.ErrNoRows) {
		err = nil
	}
	return identity.Decode(value), err
}
func (s Service) SaveWebhook(ctx context.Context, value string, clear bool) error {
	if !clear && strings.TrimSpace(value) != "" {
		if err := identity.ValidURL(strings.TrimSpace(value)); err != nil {
			return Invalid{err.Error()}
		}
	}
	return s.mutate(ctx, "webhook", func(tx rdbms.Tx) error {
		if !clear && strings.TrimSpace(value) == "" {
			return nil
		}
		if clear {
			value = ""
		}
		_, err := tx.Exec(
			ctx,
			`INSERT INTO app.webhook_config(id,webhook_url) VALUES(1,$1) ON CONFLICT(id) DO UPDATE SET webhook_url=$1`,
			identity.Encode(strings.TrimSpace(value)),
		)
		return err
	})
}
