package workflow

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"

	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage"
)

type objectCollectionFixture struct {
	c              *Controller
	ctx            context.Context
	conn           *pgx.Conn
	store          *blob.Store
	refs           []blob.Reference
	sdk            *s3.Client
	foreignVersion *string
}

func TestNativeObjectCollectionPreservesHistory(t *testing.T) {
	t.Parallel()
	fixture := newObjectCollectionFixture(t)
	c, ctx := fixture.c, fixture.ctx
	conn, refs := fixture.conn, fixture.refs
	t.Cleanup(func() { conn.Close(ctx) })
	seedCollectionReferences(t, ctx, conn, refs)
	addOrphanObjects(t, fixture)
	conn.Close(ctx)
	if err := c.stopAll(); err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("c", 40)
	fixtureRelease(t, c, id, false)
	c.State.Release = id
	c.State.Mode = "development"
	c.State.Baseline = "protected-session"
	if err := c.Collect(ctx); err == nil {
		t.Fatal("collection admitted during development")
	}
	c.State.Mode = "stopped"
	c.State.Baseline = ""
	if err := c.Collect(ctx); err != nil {
		_ = c.Logs("collection")
		t.Fatal(err)
	}
	stoppedFixture(t, c)
	fixture.conn, fixture.store = startTestStorage(t, c)
	t.Cleanup(func() { fixture.conn.Close(ctx) })
	verifyCollection(t, fixture)
}

func newObjectCollectionFixture(t *testing.T) *objectCollectionFixture {
	t.Helper()
	c := nativeController(t)
	ctx := t.Context()
	conn, store := startTestStorage(t, c)
	sdk := s3.New(
		s3.Options{
			Region: "us-east-1", BaseEndpoint: aws.String(c.endpoint()),
			UsePathStyle: true,
			Credentials:  credentials.NewStaticCredentialsProvider(c.Config.S3Access, c.Config.S3Secret, ""),
		},
	)
	return &objectCollectionFixture{
		c: c, ctx: ctx, conn: conn, store: store, refs: historicalObjects(t, ctx, conn, store), sdk: sdk,
	}
}

func historicalObjects(t *testing.T, ctx context.Context, conn *pgx.Conn, store *blob.Store) []blob.Reference {
	t.Helper()
	refs := make([]blob.Reference, 8)
	for i := range refs {
		var err error
		refs[i], err = store.Put(
			ctx, strings.NewReader(fmt.Sprintf("retained historical fixture %d", i)), "text/plain", t.TempDir(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err = storage.RegisterObject(ctx, conn, refs[i]); err != nil {
			t.Fatal(err)
		}
	}
	return refs
}

func seedCollectionReferences(t *testing.T, ctx context.Context, conn *pgx.Conn, refs []blob.Reference) {
	t.Helper()
	statements := []struct {
		sql  string
		args []any
	}{
		{
			`INSERT INTO app.courses(id,name,webdav_folder,hidden) VALUES(991,'Hidden historical fixture','/Courses/991',1)`,
			nil,
		},
		{
			`INSERT INTO app.document_revisions(id,document_id,course_id,logical_path,object_id,deleted_at) VALUES('removed','removed',991,'deleted.pdf',$1,now())`,
			[]any{refs[0].SHA256},
		},
		{
			`INSERT INTO messages.archive_sources(path,root_id,course_id,fingerprint,channel_id,status,indexed_at,object_id) VALUES('removed-export','123',991,'fixture',123,'ready','fixture',$1)`,
			[]any{refs[1].SHA256},
		},
		{
			`INSERT INTO messages.archive_media(source_path,relative_path,object_id) VALUES('removed-export','media/attachment',$1)`,
			[]any{refs[2].SHA256},
		},
		{
			`INSERT INTO app.pdf_differences(id,course_id,old_object_id,new_object_id,object_id,tool_version,status) VALUES('old-comparison',991,$1,$2,$3,'fixture','ready')`,
			[]any{refs[3].SHA256, refs[4].SHA256, refs[5].SHA256},
		},
		{`CREATE TABLE app.fixture_future_reference(object_id text REFERENCES app.objects(id) ON DELETE CASCADE)`, nil},
		{`INSERT INTO app.fixture_future_reference VALUES($1)`, []any{refs[6].SHA256}},
	}
	for _, s := range statements {
		if _, err := conn.Exec(ctx, s.sql, s.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func addOrphanObjects(t *testing.T, fixture *objectCollectionFixture) {
	t.Helper()
	ctx, sdk := fixture.ctx, fixture.sdk
	// More than a page of versions exercises deleting the page's continuation key.
	for i := 0; i < 505; i++ {
		key := fmt.Sprintf("objects/%064x", i)
		if _, err := sdk.PutObject(
			ctx,
			&s3.PutObjectInput{
				Bucket: aws.String(blob.DataBucket),
				Key:    aws.String(key),
				Body:   strings.NewReader("orphan"),
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	foreign, err := sdk.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(blob.DataBucket),
			Key:    aws.String("objects/foreign-file"),
			Body:   strings.NewReader("preserve"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.foreignVersion = foreign.VersionId
}

func verifyCollection(t *testing.T, fixture *objectCollectionFixture) {
	t.Helper()
	ctx, conn, store, refs, sdk := fixture.ctx, fixture.conn, fixture.store, fixture.refs, fixture.sdk
	var count int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM app.objects`).Scan(&count); err != nil || count != 7 {
		t.Fatal("catalog lost history or retained abandoned entry", count, err)
	}
	for _, ref := range refs[:7] {
		out, err := store.Get(ctx, ref, "")
		if err != nil {
			t.Fatal("referenced historical object deleted", err)
		}
		b, err := io.ReadAll(out.Body)
		out.Body.Close()
		if err != nil || len(b) == 0 {
			t.Fatal("historical bytes unavailable", err)
		}
	}
	if out, err := store.Get(ctx, refs[7], ""); err == nil {
		out.Body.Close()
		t.Fatal("unreferenced catalog object survived")
	}
	versions, err := sdk.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(blob.DataBucket)})
	if err != nil || len(versions.Versions) != 8 {
		t.Fatal("orphan pagination or namespace preservation failed", len(versions.Versions), err)
	}
	if _, err = sdk.HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket:    aws.String(blob.DataBucket),
			Key:       aws.String("objects/foreign-file"),
			VersionId: fixture.foreignVersion,
		},
	); err != nil {
		t.Fatal("foreign object deleted", err)
	}
}
