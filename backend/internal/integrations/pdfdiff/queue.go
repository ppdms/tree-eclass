// Package pdfdiff builds historical visual differences from immutable S3 inputs.
package pdfdiff

import (
	"context"
	"fmt"
	"strings"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/jobs"
)

const ToolVersion = "diff-pdf-0.5.3"

func Enqueue(ctx context.Context, tx database.Tx, course int64, old, next blob.Reference) (string, *string, error) {
	id := identity.Stable("diff", fmt.Sprint(course), old.SHA256, next.SHA256, ToolVersion)
	inserted, status, err := tx.PDFDifferences().EnqueueDifference(ctx, database.PDFDifferenceParams{
		ID:          id,
		CourseID:    course,
		OldObjectID: old.SHA256,
		NewObjectID: next.SHA256,
		ToolVersion: ToolVersion,
	})
	if err != nil {
		return "", nil, err
	}
	if inserted {
		if _, err = jobs.EnqueueTx(ctx, tx, "index", "pdf_diff", map[string]string{"difference_id": id}, false); err != nil {
			return "", nil, err
		}
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
		return object, database.ErrNoRows
	}
	found, err := s.Pool.PDFDifferences().ReadyDifferenceObject(ctx, id)
	if err != nil {
		return object, err
	}
	object.Bucket, object.Key, object.VersionID = found.Bucket, found.Key, found.VersionID
	object.SHA256, object.Bytes, object.MediaType = found.SHA256, found.Bytes, found.MediaType
	return object, nil
}
