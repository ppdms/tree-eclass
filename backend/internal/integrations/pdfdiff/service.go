package pdfdiff

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/rdbms"
	"tree-eclass/internal/infrastructure/storage"
)

type Runner interface {
	Run(context.Context, string, string, string) (bool, error)
}
type Service struct {
	Pool    rdbms.Pool
	Objects *blob.Store
	Temp    string
	Runner  Runner
}

func (s Service) Process(ctx context.Context, id string) (err error) {
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var oldID, newID, status, version string
	if err = s.Pool.QueryRow(ctx, `SELECT old_object_id,new_object_id,status,tool_version FROM app.pdf_differences WHERE id=$1`, id).Scan(&oldID, &newID, &status, &version); err != nil {
		return err
	}
	if status == "ready" || status == "identical" {
		return nil
	}
	if version != ToolVersion {
		return errors.New("PDF difference requires a different tool version")
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE app.pdf_differences SET status='running',error=NULL WHERE id=$1`, id); err != nil {
		return err
	}
	defer func() {
		if err != nil && parent.Err() == nil {
			failureContext, stop := context.WithTimeout(parent, 2*time.Second)
			defer stop()
			_, saved := s.Pool.Exec(
				failureContext,
				`UPDATE app.pdf_differences SET status='failed',error='Visual PDF comparison failed; the original versions remain available' WHERE id=$1 AND status='running'`,
				id,
			)
			err = errors.Join(err, saved)
		}
	}()
	dir, old, next, err := s.stage(ctx, oldID, newID)
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
	var object blob.Reference
	err := s.Pool.QueryRow(ctx, `SELECT bucket,key,version_id,sha256,bytes,media_type FROM app.objects WHERE id=$1`, id).
		Scan(&object.Bucket, &object.Key, &object.VersionID, &object.SHA256, &object.Bytes, &object.MediaType)
	if err != nil {
		return err
	}
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
	var exists string
	if err = tx.QueryRow(ctx, `SELECT id FROM app.pdf_differences WHERE id=$1 FOR UPDATE`, id).Scan(&exists); err != nil {
		return err
	}
	status := "identical"
	var objectID, alias *string
	if object != nil {
		if err = storage.RegisterObject(ctx, tx, *object); err != nil {
			return err
		}
		objectID = &object.SHA256
		value := Alias(id)
		alias = &value
	}
	if _, err = tx.Exec(ctx, `UPDATE app.pdf_differences SET status=$2,object_id=$3,error=NULL WHERE id=$1`, id, status, objectID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE app.change_record_items SET diff_webdav_path=$2 WHERE pdf_difference_id=$1`, id, alias); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE app.file_versions SET diff_webdav_path=$2 WHERE pdf_difference_id=$1`, id, alias); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
