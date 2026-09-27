package synchronization

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/queries"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/integrations/mirror"
)

var ErrBusy = errors.New("this course is already being synchronized")

func (s Service) Sync(ctx context.Context, id int64, source Source, rootURL string) (Result, error) {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return Result{}, err
	}
	defer conn.Release()
	key := fmt.Sprintf("eclass-sync:%d", id)
	var locked bool
	err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, key).Scan(&locked)
	if err != nil {
		return Result{}, err
	}
	if !locked {
		return Result{}, ErrBusy
	}
	defer unlock(conn, key)
	course, err := queries.New(s.Pool).Course(ctx, id)
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
	if err = s.publish(ctx, course, next, changes); err != nil {
		return Result{}, err
	}
	synced := result(changes)
	if s.MirrorRoot != "" {
		if _, err = s.mirrorTree(ctx, course, next); err != nil {
			return synced, err
		}
	}
	return synced, nil
}

func (s Service) mirrorTree(ctx context.Context, course queries.AppCourse, tree Tree) (mirror.Result, error) {
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

func unlock(conn *pgxpool.Conn, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, key); err != nil {
		_ = conn.Conn().Close(ctx)
	}
}

func (s Service) publish(ctx context.Context, course queries.AppCourse, tree Tree, changes []Change) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var name, prefix string
	var hidden int64
	err = tx.QueryRow(ctx, `SELECT name,webdav_folder,hidden FROM app.courses WHERE id=$1 FOR UPDATE`, course.ID).
		Scan(&name, &prefix, &hidden)
	if err != nil {
		return err
	}
	if name != course.Name || prefix != course.WebdavFolder || hidden != course.Hidden {
		return errors.New("course changed during synchronization; retry the check")
	}
	if _, err = tx.Exec(ctx, `DELETE FROM app.nodes WHERE course_id=$1`, course.ID); err != nil {
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
	if _, err = tx.Exec(ctx, `UPDATE knowledge.documents d SET is_current=0 WHERE d.course_id=$1 AND d.source_origin='eclass' AND NOT(d.id=ANY($2::text[])) AND NOT EXISTS(SELECT 1 FROM knowledge.archive_members m WHERE m.child_document_id=d.id)`, course.ID, current); err != nil {
		return err
	}
	if err = saveChanges(ctx, tx, course.ID, changes); err != nil {
		return err
	}
	if _, err = jobs.EnqueueTx(ctx, tx, "projection", "refresh_read_model", map[string]any{}, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func saveDirectories(ctx context.Context, tx pgx.Tx, courseID int64, dirs []Directory) (map[string]int64, error) {
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
		var id int64
		err := tx.QueryRow(ctx, `INSERT INTO app.nodes(course_id,parent_id,name,url,local_path) VALUES($1,$2,$3,$4,$5) RETURNING id`, courseID, parent, identity.Encode(d.Name), d.URL, identity.Encode(d.Path)).
			Scan(&id)
		if err != nil {
			return nil, err
		}
		nodes[d.Path] = id
	}
	return nodes, nil
}

func saveFile(ctx context.Context, tx pgx.Tx, nodeID int64, f File) error {
	var object, revision *string
	if f.Object != nil {
		object = &f.Object.SHA256
		revision = &f.Revision
	}
	_, err := tx.Exec(
		ctx,
		`INSERT INTO app.files(node_id,url,name,md5_hash,etag,redirect_url,last_updated,local_path,object_id,revision_id) VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9,$10)`,
		nodeID,
		f.URL,
		identity.Encode(f.Name),
		f.MD5,
		f.ETag,
		f.Redirect,
		f.Updated,
		identity.Encode(f.Path),
		object,
		revision,
	)
	return err
}
