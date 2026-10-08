package synchronization

import (
	"context"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/domain/objects"
	"tree-eclass/internal/infrastructure/jobs"
)

func publishDocument(ctx context.Context, tx database.Tx, course database.AppCourse, file File) (string, error) {
	o := file.Object
	document := identity.Document(course.ID, file.Path)
	revision := identity.Stable("rev", document, o.SHA256)
	if err := tx.Jobs().QueueLock(ctx, "document:"+document); err != nil {
		return "", err
	}
	if err := objects.RegisterObject(ctx, tx, objects.Reference{
		Bucket: o.Bucket, Key: o.Key, VersionID: o.VersionID,
		SHA256: o.SHA256, Bytes: o.Bytes, MediaType: o.MediaType,
	}); err != nil {
		return "", err
	}
	if err := tx.Objects().RegisterRevision(ctx, database.RegisterRevisionParams{
		ID:          revision,
		DocumentID:  document,
		CourseID:    course.ID,
		LogicalPath: file.Path,
		ObjectID:    o.SHA256,
	}); err != nil {
		return "", err
	}
	kind := materials.Kind(file.Path, o.MediaType)
	status := "pending"
	if kind == "" {
		kind = "unsupported"
		status = "unsupported"
	}
	status, err := tx.Indexing().ObserveEclassDocument(ctx, database.EclassDocumentParams{
		Document:        document,
		CourseID:        course.ID,
		CourseName:      course.Name,
		CourseShortName: course.ShortName,
		Path:            identity.Encode(file.Path),
		Name:            identity.Encode(file.Name),
		SHA:             o.SHA256,
		URL:             &file.URL,
		ETag:            &file.ETag,
		Media:           &o.MediaType,
		Kind:            kind,
		Bytes:           &o.Bytes,
		Status:          status,
		Updated:         &file.Updated,
	})
	if err != nil {
		return "", err
	}
	if status == "pending" {
		if _, err = jobs.EnqueueTx(ctx, tx, "index", "index_document",
			map[string]string{"document_id": document}, false); err != nil {
			return "", err
		}
	}
	return revision, nil
}
