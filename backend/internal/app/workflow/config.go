// Package workflow owns manual native lifecycle and code/data mode selection.
package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/checkpoint"
	"tree-eclass/internal/infrastructure/process"
)

const reserveBytes uint64 = 5 * 1024 * 1024 * 1024

// objectsFormat identifies the on-disk object store layout recorded in
// dataset.json and checkpoint manifests.
const objectsFormat = "fs-v1"

type Config struct {
	SourceRoot      string `json:"source_root"`
	Format          int    `json:"format"`
	PostgresBin     string `json:"postgres_bin"`
	PostgresVersion string `json:"postgres_version"`
	Bun             string `json:"bun"`
	ParserPython    string `json:"parser_python,omitempty"`
	Tessdata        string `json:"tessdata"`
	DiscordExporter string `json:"discord_exporter"`
	PDFDiff         string `json:"pdf_diff"`
	PDFDiffSHA      string `json:"pdf_diff_sha256"`
	Password        string `json:"password"`
	Ports           Ports  `json:"ports"`
}
type Ports struct{ Postgres, HTTP int }
type Selection struct {
	Mode       string    `json:"mode"`
	Release    string    `json:"release,omitempty"`
	Previous   string    `json:"previous,omitempty"`
	Baseline   string    `json:"baseline,omitempty"`
	Activation string    `json:"activation,omitempty"`
	Session    string    `json:"session"`
	Started    time.Time `json:"started"`
}
type Controller struct {
	Root, Repo, Executable string
	Config                 Config
	State                  Selection
	Processes              process.Manager
	lock                   *os.File
	pin                    string
	buildCache             string
	releasePin             string
	database               string
}

func Open(repo string) (*Controller, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	root := os.Getenv("TREE_WORKFLOW_HOME")
	if root == "" {
		root = filepath.Join(home, ".local", "share", "tree-eclass")
	}
	root, err = filepath.Abs(filepath.Join(root, "native-v1"))
	if err != nil {
		return nil, err
	}
	return openAt(repo, root)
}

func openAt(repo, root string) (*Controller, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	lock, err := platform.Lock(filepath.Join(root, "operation.lock"))
	if err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		platform.Unlock(lock)
		return nil, err
	}
	c := &Controller{Root: root, Repo: repo, Executable: executable, lock: lock}
	c.Processes = process.Manager{Root: filepath.Join(root, "processes"), Executable: executable}
	err = platform.ReadJSON(filepath.Join(root, "config.json"), &c.Config)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		c.Close()
		return nil, err
	}
	if c.Config.SourceRoot != "" && os.Getenv("TREE_SOURCE_ROOT") == "" {
		c.Repo = c.Config.SourceRoot
	}
	err = platform.ReadJSON(filepath.Join(root, "selection.json"), &c.State)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		c.Close()
		return nil, err
	}
	return c, nil
}
func (c *Controller) Close() {
	if c.lock != nil {
		platform.Unlock(c.lock)
		c.lock = nil
	}
}
func (c *Controller) active() string      { return filepath.Join(c.Root, "active") }
func (c *Controller) objectsRoot() string { return filepath.Join(c.active(), "objects") }
func (c *Controller) save() error {
	return platform.WriteJSON(filepath.Join(c.Root, "selection.json"), c.State)
}
func (c *Controller) snapshots() checkpoint.Store {
	return checkpoint.Store{Root: c.Root, Stopped: c.stopped}
}
func (c *Controller) configured() error {
	if c.Config.Format != 1 {
		return errors.New("native runtime is not configured; run ./tree setup")
	}
	return nil
}

// migratedStorage refuses to start datasets whose objects still live in the
// legacy on-disk layout; their contents must be exported into the filesystem
// object store first.
func (c *Controller) migratedStorage() error {
	if _, err := os.Stat(filepath.Join(c.active(), "seaweed")); err != nil {
		return nil
	}
	if _, err := os.Stat(c.objectsRoot()); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New(
				"this dataset predates filesystem object storage; run ./tree storage export-s3 before starting",
			)
		}
		return err
	}
	return nil
}
func (c *Controller) space() error {
	free, err := platform.Available(c.Root)
	if err != nil {
		return err
	}
	if free < reserveBytes {
		return fmt.Errorf(
			"at least 5 GiB free disk space is required; available %.2f GiB",
			float64(free)/(1024*1024*1024),
		)
	}
	return nil
}
func (c *Controller) databaseURL() string {
	u := url.URL{
		Scheme: "postgresql",
		User:   url.UserPassword("tree", c.Config.Password),
		Host:   fmt.Sprintf("127.0.0.1:%d", c.Config.Ports.Postgres),
		Path:   "/" + databaseName(c.database),
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String()
}
func databaseName(name string) string {
	if name == "" {
		return "tree"
	}
	return name
}

// Read the previous two-listener configuration without changing the user's public
// URL. Subsequent configuration writes contain only the single HTTP listener.
func (p *Ports) UnmarshalJSON(data []byte) error {
	type current Ports
	var value struct {
		current
		Frontend, API int
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*p = Ports(value.current)
	if p.HTTP == 0 {
		p.HTTP = value.Frontend
	}
	if p.HTTP == 0 {
		p.HTTP = value.API
	}
	return nil
}
