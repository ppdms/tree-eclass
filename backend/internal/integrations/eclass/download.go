package eclass

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"

	"tree-eclass/internal/domain/materials"
)

type Download struct {
	Body                            io.ReadCloser
	Name, MediaType, ETag, Redirect string
	Unchanged                       bool
}

func downloadName(resp *http.Response, fallback string) (string, error) {
	_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Disposition"))
	if err == nil && params["filename"] != "" {
		return materials.Filename(params["filename"])
	}
	if fallback == "" {
		fallback = path.Base(resp.Request.URL.Path)
	}
	return materials.Filename(fallback)
}

func (c *Client) Download(ctx context.Context, raw, fallback, etag string) (Download, error) {
	for attempt := 0; attempt < 2; attempt++ {
		resp, err := c.downloadRequest(ctx, raw, etag)
		if err != nil {
			return Download{}, err
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			_ = resp.Body.Close()
			if attempt == 1 {
				return Download{}, ErrAuthentication
			}
			if err = c.Login(ctx); err != nil {
				return Download{}, err
			}
			continue
		}
		if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") && resp.StatusCode == http.StatusOK {
			data, err := readPage(resp)
			if err != nil {
				return Download{}, err
			}
			if loginPage(data) {
				if attempt == 1 {
					return Download{}, ErrAuthentication
				}
				if err = c.Login(ctx); err != nil {
					return Download{}, err
				}
				continue
			}
			resp.Body = io.NopCloser(bytes.NewReader(data))
		}
		return c.downloadResponse(resp, fallback, etag)
	}
	return Download{}, ErrAuthentication
}

func (c *Client) downloadRequest(ctx context.Context, raw, etag string) (*http.Response, error) {
	u, err := c.base.Parse(raw)
	if err != nil || !c.sameOrigin(u) {
		return nil, errors.New("file request must stay on the eClass origin")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "tree-eclass/1 (personal course synchronization)")
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("eClass file request failed or timed out")
	}
	return resp, nil
}

func (c *Client) downloadResponse(resp *http.Response, fallback, etag string) (Download, error) {
	if resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest &&
		resp.StatusCode != http.StatusNotModified {
		defer resp.Body.Close()
		loc, err := resp.Location()
		if err != nil || loc.User != nil || (loc.Scheme != "https" && loc.Scheme != "http") || c.sameOrigin(loc) {
			return Download{}, errors.New("invalid eClass file redirect")
		}
		return Download{Redirect: loc.String(), Name: fallback, ETag: resp.Header.Get("ETag")}, nil
	}
	if resp.StatusCode == http.StatusNotModified && etag != "" {
		_ = resp.Body.Close()
		return Download{Unchanged: true, ETag: etag}, nil
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return Download{}, fmt.Errorf("eClass file returned HTTP %d", resp.StatusCode)
	}
	name, err := downloadName(resp, fallback)
	if err != nil {
		_ = resp.Body.Close()
		return Download{}, err
	}
	media, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if media == "" {
		media = "application/octet-stream"
	}
	return Download{Body: resp.Body, Name: name, MediaType: media, ETag: resp.Header.Get("ETag")}, nil
}

func driveURL(raw string) (string, error) {
	if !driveFile(raw) {
		return "", errors.New("not a Google Drive file link")
	}
	u, _ := url.Parse(raw)
	id := u.Query().Get("id")
	if strings.HasPrefix(u.Path, "/file/d/") {
		id = strings.Split(strings.TrimPrefix(u.Path, "/file/d/"), "/")[0]
	}
	if id == "" || strings.IndexFunc(id, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-')
	}) >= 0 {
		return "", errors.New("invalid Google Drive file ID")
	}
	q := url.Values{"id": {id}, "export": {"download"}}
	for _, key := range []string{"resourcekey", "authuser"} {
		if v := u.Query().Get(key); v != "" {
			q.Set(key, v)
		}
	}
	return "https://drive.usercontent.google.com/download?" + q.Encode(), nil
}

// Drive uses a separate, cookie-free client; eClass authentication cannot cross
// this boundary. HTML sign-in/confirmation pages are never stored as documents.
func (c *Client) Drive(ctx context.Context, raw, fallback string) (Download, error) {
	u, err := driveURL(raw)
	if err != nil {
		return Download{}, err
	}
	public := &http.Client{
		Timeout:   c.http.Timeout,
		Transport: c.http.Transport,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if len(via) >= 10 || r.URL.Scheme != "https" || r.URL.User != nil ||
				!(r.URL.Hostname() == "drive.usercontent.google.com" || strings.HasSuffix(r.URL.Hostname(), ".googleusercontent.com")) {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Download{}, err
	}
	resp, err := public.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return Download{}, ctx.Err()
		}
		return Download{}, errors.New("Google Drive download failed")
	}
	if resp.StatusCode != http.StatusOK || strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
		_ = resp.Body.Close()
		return Download{}, errors.New("Google Drive file is unavailable or requires confirmation")
	}
	return c.downloadResponse(resp, fallback, "")
}
