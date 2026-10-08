package pdfdiff

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/infrastructure/blob"
)

type Runner interface {
	Run(context.Context, string, string, string) (bool, error)
}
type Service struct {
	Pool    database.Store
	Objects *blob.Store
	Temp    string
	Runner  Runner
}

func (s Service) Process(ctx context.Context, id string) (err error) {
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	diff, err := s.Pool.PDFDifferences().Difference(ctx, id)
	if err != nil {
		return err
	}
	if diff.Status == "ready" || diff.Status == "identical" {
		return nil
	}
	if diff.ToolVersion != ToolVersion {
		return errors.New("PDF difference requires a different tool version")
	}
	if err = s.Pool.PDFDifferences().MarkDifferenceRunning(ctx, id); err != nil {
		return err
	}
	defer func() {
		if err != nil && parent.Err() == nil {
			failureContext, stop := context.WithTimeout(parent, 2*time.Second)
			defer stop()
			saved := s.Pool.PDFDifferences().MarkDifferenceFailed(
				failureContext,
				id,
				"Visual PDF comparison failed; the original versions remain available",
			)
			err = errors.Join(err, saved)
		}
	}()
	dir, old, next, err := s.stage(ctx, diff.OldObjectID, diff.NewObjectID)
	defer os.RemoveAll(dir)
	if err != nil {
		return err
	}
	different, err := s.Runner.Run(ctx, dir, old, next)
	if err != nil {
		return err
	}
	var object *blob.Reference
	if different {
		if object, err = s.uploadDiff(ctx, dir); err != nil {
			return err
		}
	}
	return s.publish(ctx, id, object)
}

func (s Service) stage(ctx context.Context, oldID, newID string) (dir, old, next string, err error) {
	dir, err = os.MkdirTemp(s.Temp, "pdf-diff-*")
	if err != nil {
		return "", "", "", err
	}
	old, next = filepath.Join(dir, "old.pdf"), filepath.Join(dir, "new.pdf")
	if err = s.download(ctx, oldID, old); err != nil {
		return dir, "", "", err
	}
	if err = s.download(ctx, newID, next); err != nil {
		return dir, "", "", err
	}
	return dir, old, next, nil
}

func (s Service) uploadDiff(ctx context.Context, dir string) (*blob.Reference, error) {
	file, err := os.Open(filepath.Join(dir, "difference.pdf"))
	if err != nil {
		return nil, err
	}
	ref, putErr := s.Objects.Put(ctx, file, "application/pdf", s.Temp)
	closeErr := file.Close()
	if err = errors.Join(putErr, closeErr); err != nil {
		return nil, err
	}
	return &ref, nil
}
func (s Service) download(ctx context.Context, id, target string) error {
	found, err := s.Pool.Objects().GetObject(ctx, id)
	if err != nil {
		return err
	}
	var object blob.Reference
	object.Bucket, object.Key, object.VersionID = found.Bucket, found.Key, found.VersionID
	object.SHA256, object.Bytes, object.MediaType = found.SHA256, found.Bytes, found.MediaType
	spool, err := s.Objects.Download(ctx, object, filepath.Dir(target))
	if err != nil {
		return err
	}
	defer os.Remove(spool)
	return os.Rename(spool, target)
}
func (s Service) publish(ctx context.Context, id string, object *blob.Reference) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.PDFDifferences().LockDifference(ctx, id); err != nil {
		return err
	}
	publication := database.PDFDifferencePublication{ID: id}
	if object != nil {
		if err = tx.Objects().RegisterObject(ctx, database.ObjectReference{
			Bucket:    object.Bucket,
			Key:       object.Key,
			VersionID: object.VersionID,
			SHA256:    object.SHA256,
			Bytes:     object.Bytes,
			MediaType: object.MediaType,
		}); err != nil {
			return err
		}
		publication.ObjectID = &object.SHA256
	}
	if err = tx.PDFDifferences().PublishDifference(ctx, publication); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
