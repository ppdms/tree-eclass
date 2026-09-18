// Package eclass reads the authenticated upstream course library. A client is
// scoped to one synchronization run; credentials and cookies are never logged.
package eclass

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/publicsuffix"
)

const BaseURL = "https://eclass.aueb.gr"
const maxPage = 8 << 20

var ErrAuthentication = errors.New("eClass login failed; check the saved credentials")
var ErrRegistration = errors.New("register for this course on eClass before synchronizing it")

type Client struct {
	base               *url.URL
	http               *http.Client
	username, password string
	loginMu            sync.Mutex
}

func New(base, username, password string) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("invalid eClass base URL")
	}
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return nil, err
	}
	c := &Client{base: u, username: username, password: password}
	c.http = &http.Client{Jar: jar, Timeout: 2 * time.Minute, CheckRedirect: c.redirect}
	return c, nil
}

func (c *Client) SetTimeout(timeout time.Duration) { c.http.Timeout = timeout }
func (c *Client) Close()                           { c.http.CloseIdleConnections() }

func (c *Client) redirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("eClass redirect limit exceeded")
	}
	// Off-site links are cataloged, not followed with an authenticated session.
	if !c.sameOrigin(req.URL) {
		return http.ErrUseLastResponse
	}
	return nil
}

func (c *Client) sameOrigin(u *url.URL) bool {
	return u.User == nil && strings.EqualFold(u.Host, c.base.Host) && u.Scheme == c.base.Scheme
}

func (c *Client) Resolve(raw string) (string, error) {
	u, err := c.base.Parse(raw)
	if err != nil || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return "", errors.New("invalid upstream link")
	}
	u.Fragment = ""
	return u.String(), nil
}

func (c *Client) request(ctx context.Context, method, raw string, form url.Values) (*http.Response, error) {
	u, err := c.base.Parse(raw)
	if err != nil || !c.sameOrigin(u) {
		return nil, errors.New("authenticated request must stay on the eClass origin")
	}
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "tree-eclass/1 (personal course synchronization)")
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// url.Error can contain RSS tokens; do not persist it in queue diagnostics.
		return nil, errors.New("eClass request failed or timed out")
	}
	return resp, nil
}

func readPage(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("eClass returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxPage+1))
	if err == nil && len(data) > maxPage {
		err = errors.New("eClass page exceeds 8 MiB")
	}
	return data, err
}

func loginPage(data []byte) bool {
	s := string(data)
	return strings.Contains(s, "login_page=1") && strings.Contains(s, "uname")
}

func (c *Client) Login(ctx context.Context) error {
	c.loginMu.Lock()
	defer c.loginMu.Unlock()
	if c.username == "" || c.password == "" {
		return ErrAuthentication
	}
	// eClass checks that its session cookie returns with the login form. Posting
	// credentials into a fresh cookie jar produces a cookies-disabled rejection.
	resp, err := c.request(ctx, http.MethodGet, "/main/login_form.php", nil)
	if err != nil {
		return err
	}
	if _, err = readPage(resp); err != nil {
		return err
	}
	resp, err = c.request(
		ctx,
		http.MethodPost,
		"/?login_page=1",
		url.Values{"uname": {c.username}, "pass": {c.password}, "submit": {"Είσοδος"}},
	)
	if err != nil {
		return err
	}
	data, err := readPage(resp)
	if err != nil {
		return err
	}
	if loginPage(data) {
		return ErrAuthentication
	}
	for _, cookie := range c.http.Jar.Cookies(c.base) {
		if cookie.Name == "PHPSESSID" && cookie.Value != "" {
			return nil
		}
	}
	return ErrAuthentication
}

func (c *Client) Page(ctx context.Context, raw string) ([]byte, error) {
	for attempt := 0; attempt < 2; attempt++ {
		resp, err := c.request(ctx, http.MethodGet, raw, nil)
		if err != nil {
			return nil, err
		}
		reauth := resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden
		data, err := readPage(resp)
		if reauth || (err == nil && loginPage(data)) {
			if attempt == 1 {
				return nil, ErrAuthentication
			}
			if err = c.Login(ctx); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		if strings.Contains(string(data), "Εγγραφή και είσοδος στο μάθημα") {
			return nil, ErrRegistration
		}
		return data, nil
	}
	return nil, ErrAuthentication
}
