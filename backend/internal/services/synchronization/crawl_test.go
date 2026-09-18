package synchronization

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"tree-eclass/internal/integrations/eclass"
)

type boundedSource struct {
	PageBody  []byte
	Downloads int
}

func (s *boundedSource) Page(context.Context, string) ([]byte, error) { return s.PageBody, nil }
func (s *boundedSource) Download(context.Context, string, string, string) (eclass.Download, error) {
	s.Downloads++
	return eclass.Download{}, errors.New("unexpected download")
}
func (s *boundedSource) Drive(ctx context.Context, a, b string) (eclass.Download, error) {
	return s.Download(ctx, a, b, "")
}

func TestOversizedFinalDirectoryIsRejectedBeforeDownloading(t *testing.T) {
	var page strings.Builder
	page.WriteString("<title>Έγγραφα</title>")
	for i := range 20001 {
		fmt.Fprintf(
			&page,
			`<a href="/modules/document/file.php?course=INF101&amp;download=/file-%d.txt">file-%d.txt</a>`,
			i,
			i,
		)
	}
	source := &boundedSource{PageBody: []byte(page.String())}
	_, err := (Service{}).crawl(
		t.Context(),
		source,
		Directory{Path: "/Courses/101/eclass", URL: "https://example.invalid/modules/document/index.php?course=INF101"},
		Tree{},
	)
	if err == nil || !strings.Contains(err.Error(), "20000") || source.Downloads != 0 {
		t.Fatal("single-page limit escaped admission", source.Downloads, err)
	}
}
