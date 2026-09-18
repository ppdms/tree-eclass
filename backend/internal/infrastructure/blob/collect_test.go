package blob

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCollectionRefusesUnverifiedOrPartialDeletion(t *testing.T) {
	for _, scenario := range []string{"database-error", "incomplete-reference-check", "registered", "partial-delete"} {
		t.Run(scenario, func(t *testing.T) {
			deletes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				if r.Method == http.MethodGet {
					_, _ = w.Write(
						[]byte(
							`<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>objects/` + strings.Repeat(
								"a",
								64,
							) + `</Key><VersionId>fixture-version</VersionId><Size>7</Size></Version></ListVersionsResult>`,
						),
					)
					return
				}
				deletes++
				_, _ = w.Write(
					[]byte(
						`<DeleteResult><Error><Key>objects/` + strings.Repeat(
							"a",
							64,
						) + `</Key><VersionId>fixture-version</VersionId><Code>AccessDenied</Code></Error></DeleteResult>`,
					),
				)
			}))
			defer server.Close()
			store, err := New(server.URL, "synthetic-access", "synthetic-secret")
			if err != nil {
				t.Fatal(err)
			}
			result, err := store.PruneUnregistered(
				t.Context(),
				func(context.Context, []string, []string) ([]bool, error) {
					switch scenario {
					case "database-error":
						return nil, errors.New("database unavailable")
					case "incomplete-reference-check":
						return nil, nil
					case "registered":
						return []bool{true}, nil
					default:
						return []bool{false}, nil
					}
				},
			)
			if (err == nil) != (scenario == "registered") {
				t.Fatal("incorrect completion", err)
			}
			if result.Versions != 0 || result.Bytes != 0 {
				t.Fatal("unacknowledged deletion counted", result)
			}
			if (deletes > 0) != (scenario == "partial-delete") {
				t.Fatal("unverified object deletion", deletes)
			}
		})
	}
}
