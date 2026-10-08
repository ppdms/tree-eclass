package settings

import (
	"context"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type Credentials struct {
	Username string `json:"username"`
	Password string `json:"-"`
}

func readCredentials(ctx context.Context, db database.Operations) (*Credentials, error) {
	stored, err := db.Settings().LoadCredentials(ctx)
	if database.IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &Credentials{
		Username: identity.Decode(stored.Username),
		Password: identity.Decode(stored.Password),
	}, nil
}
func (s Service) Credentials(ctx context.Context) (*Credentials, error) {
	return readCredentials(ctx, s.Pool)
}
func (s Service) SaveCredentials(ctx context.Context, username, password string, clear bool) error {
	username = strings.TrimSpace(username)
	if username == "" {
		return Invalid{"Username is required"}
	}
	return s.mutate(ctx, "credentials", func(tx database.Tx) error {
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
		if err := tx.Settings().SaveCredentials(ctx, database.SettingsCredentials{
			Username: identity.Encode(username),
			Password: identity.Encode(final),
		}); err != nil {
			return err
		}
		// Saved cookies must not authenticate the previous credential pair.
		if existing == nil || existing.Username != username || existing.Password != final {
			return tx.Settings().ClearSessionCookie(ctx)
		}
		return nil
	})
}
func (s Service) Webhook(ctx context.Context) (string, error) {
	encoded, err := s.Pool.Settings().LoadWebhook(ctx)
	if database.IsNoRows(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return identity.Decode(encoded), nil
}
func (s Service) SaveWebhook(ctx context.Context, value string, clear bool) error {
	if !clear && strings.TrimSpace(value) != "" {
		if err := identity.ValidURL(strings.TrimSpace(value)); err != nil {
			return Invalid{err.Error()}
		}
	}
	return s.mutate(ctx, "webhook", func(tx database.Tx) error {
		if !clear && strings.TrimSpace(value) == "" {
			return nil
		}
		if clear {
			value = ""
		}
		return tx.Settings().SaveWebhook(ctx, identity.Encode(strings.TrimSpace(value)))
	})
}
