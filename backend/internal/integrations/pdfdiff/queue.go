// Package pdfdiff builds historical visual differences from immutable S3 inputs.
package pdfdiff

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/jobs"
)

const ToolVersion = "diff-pdf-0.5.3"

func Enqueue(ctx context.Context, tx pgx.Tx, course int64, old, next blob.Reference) (string, *string, error) {
	id := identity.Stable("diff", fmt.Sprint(course), old.SHA256, next.SHA256, ToolVersion)
	result, err := tx.Exec(
		ctx,
		`INSERT INTO app.pdf_differences(id,course_id,old_object_id,new_object_id,tool_version) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO NOTHING`,
		id,
		course,
		old.SHA256,
		next.SHA256,
		ToolVersion,
	)
	if err != nil {
		return "", nil, err
	}
	if result.RowsAffected() > 0 {
		if _, err = jobs.EnqueueTx(ctx, tx, "index", "pdf_diff", map[string]string{"difference_id": id}, false); err != nil {
			return "", nil, err
		}
	}
	var status string
	if err = tx.QueryRow(ctx, `SELECT status FROM app.pdf_differences WHERE id=$1`, id).Scan(&status); err != nil {
		return "", nil, err
	}
	var path *string
	if status == "ready" {
		value := Alias(id)
		path = &value
	}
	return id, path, nil
}
func Alias(id string) string { return "/_diffs/" + id + ".pdf" }
func (s Service) Content(ctx context.Context, path string) (blob.Reference, error) {
	var object blob.Reference
	id := strings.TrimSuffix(strings.TrimPrefix(path, "/_diffs/"), ".pdf")
	if len(id) != 37 || Alias(id) != path {
		return object, pgx.ErrNoRows
	}
	err := s.Pool.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type FROM app.pdf_differences d JOIN app.objects o ON o.id=d.object_id JOIN app.courses c ON c.id=d.course_id AND c.hidden=0 WHERE d.id=$1 AND d.status='ready'`, id).
		Scan(&object.Bucket, &object.Key, &object.VersionID, &object.SHA256, &object.Bytes, &object.MediaType)
	return object, err
}
