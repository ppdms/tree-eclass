package library

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"tree-eclass/internal/infrastructure/rdbms"

	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/domain/messages"
)

var ErrResource = errors.New("unknown library resource")

func (r *Registry) Resource(ctx context.Context, uri string) (any, error) {
	if len(uri) > 4096 {
		return nil, ErrResource
	}
	u, err := url.Parse(uri)
	if err != nil || u.User != nil || u.ForceQuery || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, ErrResource
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if u.Scheme == "eclass" && u.Host == "courses" {
		if u.Path == "" {
			return r.courses(ctx)
		}
		id, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || id < 1 {
			return nil, ErrResource
		}
		if len(parts) == 1 {
			return r.course(ctx, id)
		}
		if len(parts) == 2 && parts[1] == "guide" {
			return r.guide(ctx, id)
		}
	}
	if u.Scheme == "eclass" && u.Host == "documents" && parts[0] != "" {
		return r.documentResource(ctx, parts)
	}
	if u.Scheme == "discord" && u.Host == "conversations" && len(parts) == 1 && parts[0] != "" {
		return (messages.Reader{Pool: r.Pool}).Read(ctx, parts[0], 1, 1)
	}
	return nil, ErrResource
}

func (r *Registry) documentResource(ctx context.Context, parts []string) (any, error) {
	k := knowledge.Reader{Pool: r.Pool}
	switch {
	case len(parts) == 1:
		return k.Document(ctx, parts[0])
	case len(parts) == 2 && parts[1] == "insight":
		return k.MaterialInsight(ctx, parts[0])
	case len(parts) == 4 && parts[1] == "pages" && parts[3] == "insight":
		page, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil || page < 1 {
			return nil, ErrResource
		}
		return k.PageInsight(ctx, parts[0], page)
	case len(parts) == 3 && parts[1] == "units":
		kind, start, ok := strings.Cut(parts[2], ":")
		if !ok || kind == "" || start == "" || len(kind) > 64 || len(start) > 128 {
			return nil, ErrResource
		}
		return k.Read(
			ctx,
			knowledge.ReadRequest{
				DocumentID:    parts[0],
				Locators:      []knowledge.Locator{{Type: kind, Start: start}},
				MaxCharacters: 30000,
			},
		)
	}
	return nil, ErrResource
}

func (r *Registry) course(ctx context.Context, id int64) (map[string]any, error) {
	value, err := r.courses(ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range value.(map[string]any)["courses"].([]map[string]any) {
		if c["course_id"] == id {
			return c, nil
		}
	}
	return nil, rdbms.ErrNoRows
}

func (r *Registry) guide(ctx context.Context, id int64) (any, error) {
	course, err := r.course(ctx, id)
	if err != nil {
		return nil, err
	}
	materials, err := (knowledge.Reader{Pool: r.Pool}).Guide(ctx, id)
	if err != nil {
		return nil, err
	}
	materials["course"], materials["derived"], materials["generation"] = course, true, "deterministic metadata and headings"
	return materials, nil
}
