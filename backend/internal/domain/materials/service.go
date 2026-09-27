// Package materials publishes verified immutable objects and their catalog rows.
package materials

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/text/unicode/norm"
	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/objects"
	"tree-eclass/internal/domain/queries"
)

var ErrDuplicate = errors.New("a file with this name already exists")

var TypeFolders = map[string]string{
	"past_paper":        "past-papers",
	"student_notes":     "student-notes",
	"study_guide":       "study-guides",
	"textbook":          "textbooks",
	"exercise_solution": "exercises-solutions",
	"lecture_material":  "lecture-material",
	"other":             "other",
}

type ObjectWriter interface {
	Put(context.Context, io.Reader, string, string) (objects.Reference, error)
}
type Service struct {
	Pool    *pgxpool.Pool
	Objects ObjectWriter
	Temp    string
}
type Upload struct {
	CourseID              int64
	Name, Type, MediaType string
	Body                  io.Reader
}
type Result struct {
	DocumentID, RevisionID, Path, Kind, CommandID string
	Object                                        objects.Reference
}

func Filename(raw string) (string, error) {
	name := norm.NFC.String(strings.TrimSpace(raw))
	if name == "" || name == "." || name == ".." || utf8.RuneCountInString(name) > 240 {
		return "", errors.New("choose a filename of at most 240 characters")
	}
	for _, r := range name {
		if r == '/' || r == '\\' || unicode.IsControl(r) {
			return "", errors.New("filename cannot contain separators or control characters")
		}
	}
	return name, nil
}
func Kind(name, mediaType string) string {
	extension := strings.ToLower(path.Ext(name))
	for kind, extensions := range map[string]string{"pdf": ".pdf", "image": ".jpg .jpeg .png", "presentation": ".pptx", "document": ".docx", "spreadsheet": ".xlsx", "html": ".html .htm", "notebook": ".ipynb", "archive": ".zip .rar .tar .tgz .gz", "source": ".py .js .ts .java .c .h .cpp .hpp .go .rs .sql .sh .css .tex", "text": ".txt .md .rst .csv .tsv .json .xml .yaml .yml"} {
		for _, candidate := range strings.Fields(extensions) {
			if extension == candidate {
				return kind
			}
		}
	}
	if strings.HasPrefix(mediaType, "text/") {
		return "text"
	}
	return ""
}

func (s Service) Upload(ctx context.Context, upload Upload) (Result, error) {
	var result Result
	name, err := Filename(upload.Name)
	if err != nil {
		return result, err
	}
	kind := Kind(name, upload.MediaType)
	if kind == "" {
		return result, errors.New("unsupported file type")
	}
	folder := "inbox"
	if upload.Type != "" {
		var ok bool
		folder, ok = TypeFolders[upload.Type]
		if !ok {
			return result, errors.New("choose a valid document type")
		}
	}
	course, err := queries.New(s.Pool).Course(ctx, upload.CourseID)
	if err != nil {
		return result, err
	}
	if course.Hidden != 0 {
		return result, pgx.ErrNoRows
	}
	logical := identity.Path(path.Join(course.WebdavFolder, "external", folder, name))
	document := identity.Document(upload.CourseID, logical)
	// Duplicate catalog admission is serialized in the transaction after upload;
	// a losing race leaves only an unreachable object, never an invalid reference.
	object, err := s.Objects.Put(ctx, upload.Body, upload.MediaType, s.Temp)
	if err != nil {
		return result, err
	}
	result = Result{
		DocumentID: document,
		RevisionID: identity.Stable("rev", document, object.SHA256),
		Path:       logical,
		Kind:       kind,
		Object:     object,
	}
	result.CommandID, err = s.publish(ctx, course, upload, result, name)
	return result, err
}

func (s Service) publish(
	ctx context.Context,
	course queries.AppCourse,
	upload Upload,
	result Result,
	name string,
) (string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var current int64
	if err = tx.QueryRow(ctx, `SELECT id FROM app.courses WHERE id=$1 AND hidden=0 AND name=$2 AND webdav_folder=$3 FOR SHARE`, course.ID, course.Name, course.WebdavFolder).Scan(&current); err != nil {
		return "", err
	}
	q := queries.New(tx)
	if err = q.QueueLock(ctx, "document:"+result.DocumentID); err != nil {
		return "", err
	}
	if _, err = q.IndexDocument(ctx, result.DocumentID); err == nil {
		return "", ErrDuplicate
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if err = result.stage(ctx, tx, q, course); err != nil {
		return "", err
	}
	if err = observeUpload(ctx, q, course, upload, result, name); err != nil {
		return "", err
	}
	command, err := commands.EnqueueTx(
		ctx,
		tx,
		"index",
		"index_document",
		map[string]string{"document_id": result.DocumentID},
		false,
	)
	if err != nil {
		return "", err
	}
	return command, tx.Commit(ctx)
}

func (result Result) stage(ctx context.Context, tx pgx.Tx, q *queries.Queries, course queries.AppCourse) error {
	if err := objects.RegisterObject(ctx, tx, result.Object); err != nil {
		return err
	}
	return q.RegisterRevision(
		ctx,
		queries.RegisterRevisionParams{
			ID:          result.RevisionID,
			DocumentID:  result.DocumentID,
			CourseID:    course.ID,
			LogicalPath: result.Path,
			ObjectID:    result.Object.SHA256,
		},
	)
}

func observeUpload(
	ctx context.Context,
	q *queries.Queries,
	course queries.AppCourse,
	upload Upload,
	result Result,
	name string,
) error {
	err := q.ObserveDocument(
		ctx,
		queries.ObserveDocumentParams{
			ID:              result.DocumentID,
			CourseID:        course.ID,
			CourseName:      course.Name,
			CourseShortName: course.ShortName,
			SourcePath:      identity.Encode(result.Path),
			NormalizedPath:  identity.Encode(identity.Path(result.Path)),
			DisplayName:     identity.Encode(name),
			SourceHash:      result.Object.SHA256,
			MimeType:        &result.Object.MediaType,
			DocumentKind:    result.Kind,
			SourceSizeBytes: &result.Object.Bytes,
		},
	)
	if err != nil {
		return err
	}
	if upload.Type == "" {
		return nil
	}
	err = q.MaterialMetadata(
		ctx,
		queries.MaterialMetadataParams{
			CourseID:     course.ID,
			SourcePath:   identity.Encode(result.Path),
			MaterialType: upload.Type,
		},
	)
	return err
}
