package synchronization

import (
	"context"
	"errors"
	"path"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/integrations/mirror"
)

var ErrBusy = errors.New("this course is already being synchronized")

func (s Service) Sync(ctx context.Context, id int64, source Source, rootURL string) (Result, error) {
	// Crawl outside any write transaction: network I/O takes minutes and
	// must never hold the single sqlite writer slot (or a postgres row
	// lock) across the crawl. The short publish transaction below admits
	// the course while holding the lock.
	course, err := s.Pool.Courses().Course(ctx, id)
	if err != nil {
		return Result{}, err
	}
	old, err := s.oldTree(ctx, id)
	if err != nil {
		return Result{}, err
	}
	root := Directory{
		Path: path.Join(identity.Decode(course.WebdavFolder), "eclass"),
		Name: identity.Decode(course.Name),
		URL:  rootURL,
	}
	next, err := s.crawl(ctx, source, root, old)
	if err != nil {
		return Result{}, err
	}
	changes := Diff(old, next, root.Path)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback(ctx)
	locked, err := tx.Sync().TryLockCourse(ctx, id)
	if err != nil {
		return Result{}, err
	}
	if !locked {
		return Result{}, ErrBusy
	}
	if err = s.publishLocked(ctx, tx, course, next, changes); err != nil {
		return Result{}, err
	}
	synced := result(changes)
	if s.MirrorRoot != "" {
		if _, err = s.mirrorTree(ctx, course, next); err != nil {
			return synced, err
		}
	}
	return synced, tx.Commit(ctx)
}

func (s Service) mirrorTree(ctx context.Context, course database.AppCourse, tree Tree) (mirror.Result, error) {
	files := make([]mirror.File, 0, len(tree.Files))
	for _, file := range tree.Files {
		files = append(files, mirror.File{Path: file.Path, Object: file.Object, Redirect: file.Redirect})
	}
	sourceRoot := path.Join(identity.Decode(course.WebdavFolder), "eclass")
	result, err := (mirror.Service{Root: s.MirrorRoot, Objects: s.MirrorObjects}).Sync(
		ctx,
		mirror.Course{
			ID: course.ID, Name: identity.Decode(course.Name), Hidden: course.Hidden != 0,
		},
		mirror.EClassSubtree,
		sourceRoot,
		files,
	)
	if err != nil {
		return result, err
	}
	external, err := s.mirrorExternal(ctx, course)
	if err != nil {
		return result, err
	}
	result.Files += external.Files
	result.Added += external.Added
	result.Updated += external.Updated
	result.Removed += external.Removed
	result.Unchanged += external.Unchanged
	result.Redirects += external.Redirects
	result.Bytes += external.Bytes
	return result, nil
}

func (s Service) publishLocked(
	ctx context.Context,
	tx database.Tx,
	course database.AppCourse,
	tree Tree,
	changes []Change,
) error {
	guard, err := tx.Sync().LockCourseForSync(ctx, course.ID)
	if err != nil {
		return err
	}
	if guard.Name != course.Name || guard.WebdavFolder != course.WebdavFolder || guard.Hidden != course.Hidden {
		return errors.New("course changed during synchronization; retry the check")
	}
	if err = tx.Sync().DeleteTree(ctx, course.ID); err != nil {
		return err
	}
	nodes, err := saveDirectories(ctx, tx, course.ID, tree.Directories)
	if err != nil {
		return err
	}
	current := []string{}
	for _, file := range tree.Files {
		if file.Object != nil {
			file.Revision, err = publishDocument(ctx, tx, course, file)
			if err != nil {
				return err
			}
			current = append(current, identity.Document(course.ID, file.Path))
		}
		if err = saveFile(ctx, tx, nodes[file.Parent], file); err != nil {
			return err
		}
	}
	if err = tx.Sync().RetireMissingEclassDocuments(ctx, course.ID, current); err != nil {
		return err
	}
	if err = saveChanges(ctx, tx, course.ID, changes); err != nil {
		return err
	}
	if _, err = jobs.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	return nil
}

func saveDirectories(ctx context.Context, tx database.Tx, courseID int64, dirs []Directory) (map[string]int64, error) {
	nodes := map[string]int64{}
	for _, d := range dirs {
		var parent *int64
		if d.Parent != "" {
			id, ok := nodes[d.Parent]
			if !ok {
				return nil, errors.New("crawl has an unknown parent directory")
			}
			parent = &id
		}
		id, err := tx.Sync().InsertDirectory(ctx, database.SyncDirectoryInput{
			CourseID: courseID,
			ParentID: parent,
			Name:     identity.Encode(d.Name),
			URL:      d.URL,
			Path:     identity.Encode(d.Path),
		})
		if err != nil {
			return nil, err
		}
		nodes[d.Path] = id
	}
	return nodes, nil
}

func saveFile(ctx context.Context, tx database.Tx, nodeID int64, f File) error {
	var object, revision *string
	if f.Object != nil {
		object = &f.Object.SHA256
		revision = &f.Revision
	}
	return tx.Sync().InsertFile(ctx, database.SyncFileInput{
		NodeID:     nodeID,
		URL:        f.URL,
		Name:       identity.Encode(f.Name),
		MD5:        f.MD5,
		ETag:       f.ETag,
		Redirect:   f.Redirect,
		Updated:    f.Updated,
		Path:       identity.Encode(f.Path),
		ObjectID:   object,
		RevisionID: revision,
	})
}
