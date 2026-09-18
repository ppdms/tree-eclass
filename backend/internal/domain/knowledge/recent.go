package knowledge

import (
	"context"
	"time"

	"tree-eclass/internal/domain/identity"
)

func (s Reader) Recent(ctx context.Context, requested []int64, since string, limit int) (map[string]any, error) {
	ids, err := s.Visible(ctx, requested)
	if err != nil {
		return nil, err
	}
	stamp := time.Unix(0, 0)
	if since != "" {
		stamp, err = time.Parse(time.RFC3339, since)
		if err != nil {
			return nil, err
		}
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := statusRows(
		ctx,
		tx,
		`SELECT to_jsonb(v) FROM(SELECT r.course_id,c.name course_name,r.timestamp,r.change_no,
 i.change_type,i.file_path,i.display_name,i.redirect_url,i.diff_webdav_path,true untrusted_content
 FROM app.change_record_items i JOIN app.change_records r ON r.id=i.change_record_id
 JOIN app.courses c ON c.id=r.course_id AND c.hidden=0 WHERE r.course_id=ANY($1::bigint[]) AND r.timestamp::timestamptz>=$2
 ORDER BY r.timestamp DESC,i.id DESC LIMIT $3) v`,
		ids,
		stamp,
		min(200, max(1, limit)),
	)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		for _, key := range []string{"file_path", "redirect_url", "diff_webdav_path"} {
			if text, ok := row[key].(string); ok {
				row[key] = identity.Decode(text)
			}
		}
	}
	return map[string]any{"changes": rows, "untrusted_content_notice": UntrustedNotice}, tx.Commit(ctx)
}
