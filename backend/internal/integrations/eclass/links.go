package eclass

import (
	"bytes"
	"errors"
	"net/url"
	"path"
	"strings"

	"golang.org/x/net/html"
)

type Link struct{ URL, Name string }
type Links struct{ Files, Directories []Link }

func driveFile(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Hostname() == "drive.google.com" &&
		(strings.HasPrefix(u.Path, "/file/") || (u.Path == "/open" && u.Query().Get("id") != ""))
}

func driveLinks(doc *html.Node, out *Links) map[*html.Node]bool {
	processed := map[*html.Node]bool{}
	for _, row := range elements(doc, "tr") {
		for _, a := range elements(row, "a") {
			if !driveFile(attr(a, "href")) {
				continue
			}
			name := "Unknown Google Drive File"
			if title := first(row, "a", "fileURL"); title != nil {
				name = nodeText(title)
				processed[row] = true
			}
			out.Files = append(out.Files, Link{attr(a, "href"), name})
		}
	}
	return processed
}

func inDriveRow(a *html.Node, rows map[*html.Node]bool) bool {
	if !class(a, "fileURL") {
		return false
	}
	for n := range a.Ancestors() {
		if n.Data == "tr" {
			return rows[n]
		}
	}
	return false
}

func skipDocumentLink(u, current *url.URL, name string) bool {
	if u.String() == current.String() || u.Host != current.Host || u.Scheme != current.Scheme {
		return true
	}
	if !strings.Contains(u.Path, "/modules/document/") || strings.Contains(name, "Αποθήκευση") ||
		strings.Contains(name, "Λήψη") {
		return true
	}
	q := u.Query()
	// Index-page download buttons use changing request tokens and duplicate the
	// canonical file.php links (or export whole folders). They are not documents.
	if strings.HasSuffix(u.Path, "/index.php") && q.Has("download") {
		return true
	}
	if q.Has("sort") || q.Get("openDir") == "/" {
		return true
	}
	if q.Get("course") != "" && q.Get("course") != current.Query().Get("course") {
		return true
	}
	if u.Path == "/modules/document/" ||
		(strings.HasSuffix(u.Path, "index.php") && !q.Has("download") && q.Get("openDir") == "") {
		return true
	}
	return false
}

func ParseLinks(data []byte, page string) (Links, error) {
	out := Links{Files: []Link{}, Directories: []Link{}}
	current, err := url.Parse(page)
	if err != nil {
		return out, err
	}
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return out, err
	}
	// A login/error page must never be accepted as an empty successful crawl.
	if !strings.Contains(string(data), "Έγγραφα") {
		return out, errors.New("eClass response is not a documents page")
	}
	rows := driveLinks(doc, &out)
	seen := map[string]bool{}
	for _, a := range elements(doc, "a") {
		u, err := current.Parse(attr(a, "href"))
		if err != nil || inDriveRow(a, rows) {
			continue
		}
		u.Fragment = ""
		name := nodeText(a)
		if skipDocumentLink(u, current, name) || seen[u.String()] {
			continue
		}
		seen[u.String()] = true
		link := Link{u.String(), name}
		if strings.Contains(u.Path, "/file.php") || u.Query().Has("download") ||
			path.Ext(u.Path) != "" && !strings.HasSuffix(u.Path, ".php") {
			out.Files = append(out.Files, link)
		} else if u.Query().Get("openDir") != "" {
			out.Directories = append(out.Directories, link)
		}
	}
	return out, nil
}
