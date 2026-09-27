// Package server assembles the Go API and its explicitly owned dependencies.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/infrastructure/process"
	"tree-eclass/internal/infrastructure/quota"
	"tree-eclass/internal/infrastructure/storage"
	"tree-eclass/internal/infrastructure/webui"
	"tree-eclass/internal/integrations/inference"
	"tree-eclass/internal/integrations/parser"
	"tree-eclass/internal/services/chat"
	"tree-eclass/internal/services/library"
)

type Config struct {
	AllowedHosts     []string          `json:"allowed_hosts"`
	AllowedOrigins   []string          `json:"allowed_origins"`
	DatabaseURL      string            `json:"database_url"`
	ObjectsRoot      string            `json:"objects_root"`
	Address          string            `json:"address"`
	Mode             string            `json:"mode"`
	Release          string            `json:"release"`
	Session          string            `json:"session"`
	Code             string            `json:"code"`
	Temp             string            `json:"temp"`
	ParserPython     string            `json:"parser_python"`
	ParserRoot       string            `json:"parser_root"`
	NativeToolsRoot  string            `json:"native_tools_root"`
	Tessdata         string            `json:"tessdata"`
	DiscordExporter  string            `json:"discord_exporter"`
	PDFDiff          string            `json:"pdf_diff"`
	PDFDiffSHA       string            `json:"pdf_diff_sha256"`
	ProviderKeys     map[string]string `json:"provider_keys,omitempty"`
	ExternalWorkers  bool              `json:"external_workers"`
	FrontendDir      string            `json:"frontend_dir,omitempty"`
	StorageNamespace string            `json:"storage_namespace,omitempty"`
}

func Load(path string) (Config, error) {
	var cfg Config
	err := platform.ReadJSON(path, &cfg)
	return cfg, err
}

type Server struct {
	config         Config
	db             *storage.Database
	blobs          *blob.Store
	mux            *http.ServeMux
	exportMu       sync.Mutex
	askMu          sync.Mutex
	inference      chat.Streamer
	inferenceClose func()
	expensiveMu    sync.Mutex
	parser         *parser.Runner
	library        *library.Registry
	ui             *webui.Handler
}

func New(ctx context.Context, cfg Config, options ...Option) (*Server, error) {
	if cfg.Mode != "stable" && cfg.Mode != "development" && cfg.Mode != "test" {
		return nil, errors.New("invalid runtime mode")
	}
	if cfg.Mode != "test" && cfg.Session == "" {
		return nil, errors.New("runtime session is required")
	}
	db, err := storage.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.Code != "" {
		_, err = db.Pool.Exec(
			ctx,
			`INSERT INTO app.native_settings(key,value) VALUES('_runtime_code',to_jsonb($1::text))
			ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=now() WHERE app.native_settings.value IS DISTINCT FROM excluded.value`,
			cfg.Code,
		)
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	blobs, err := blob.New(cfg.ObjectsRoot)
	if err != nil {
		db.Close()
		return nil, err
	}
	registry, err := library.New(db.Pool)
	if err != nil {
		db.Close()
		return nil, err
	}
	s := &Server{library: registry, config: cfg, db: db, blobs: blobs, mux: http.NewServeMux()}
	for _, option := range options {
		option(s)
	}
	if s.inference == nil {
		client := inference.New()
		s.inference = &quota.Guard{
			Client: client,
			Probe:  quota.HTTPProbe{Client: client.HTTP, OllamaCookie: cfg.ProviderKeys["OLLAMA_COOKIE_HEADER"]},
			Store:  quota.Postgres{Pool: db.Pool},
		}
		s.inferenceClose = client.Close
	}
	s.parser = parser.New(cfg.ParserPython, cfg.ParserRoot, cfg.Temp)
	s.parser.Tessdata = cfg.Tessdata
	s.parser.ToolsRoot = cfg.NativeToolsRoot
	s.parser.Registry = filepath.Join(cfg.Temp, ".helpers")
	s.routes()
	s.mux.Handle("/mcp", registry.HTTP())
	s.mux.Handle("/mcp/{$}", registry.HTTP())
	if cfg.FrontendDir != "" {
		s.ui, err = webui.New(cfg.FrontendDir, cfg.Session, cfg.StorageNamespace, cfg.Mode)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.mux.Handle("/", s.ui)
	}
	return s, nil
}
func (s *Server) Close() {
	if s.ui != nil {
		_ = s.ui.Close()
	}
	if s.inferenceClose != nil {
		s.inferenceClose()
	}
	s.db.Close()
}

func Serve(ctx context.Context, cfg Config) error {
	s, err := New(ctx, cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	if err := (jobs.Queue{Pool: s.db.Pool}).Recover(ctx); err != nil {
		return err
	}
	if err := s.analysisService().Recover(ctx); err != nil {
		return err
	}
	if err := s.synthesisService().Recover(ctx); err != nil {
		return err
	}
	group, active := errgroup.WithContext(ctx)
	httpServer := &http.Server{
		Addr:              cfg.Address,
		Handler:           s,
		BaseContext:       func(net.Listener) context.Context { return active },
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    64 * 1024,
	}
	group.Go(func() error { return s.indexWorker(active) })
	group.Go(func() error { return s.analysisWorker(active) })
	group.Go(func() error { return s.projectionWorker(active) })
	group.Go(func() error { return s.syncWorker(active) })
	group.Go(func() error { return s.discordWorker(active) })
	group.Go(func() error { return s.notificationWorker(active) })
	group.Go(func() error { return s.watchOwnership(active) })
	group.Go(func() error {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	group.Go(func() error {
		<-active.Done()
		stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(stop); err != nil {
			_ = httpServer.Close()
			return err
		}
		return nil
	})
	return group.Wait()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.trusted(r) {
		writeFailure(w, http.StatusForbidden, "Untrusted host")
		return
	}
	if !s.currentRuntime(w, r) {
		return
	}
	s.mux.ServeHTTP(w, r)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Debug("response write failed", "error", err)
	}
}
func writeFailure(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"detail": message})
}
func (s *Server) internal(w http.ResponseWriter, err error) {
	slog.Error("request failed", "error", err)
	writeFailure(w, http.StatusInternalServerError, "Could not complete the request")
}

// Run starts the server command and its private process-control subcommands.
func Run(ctx context.Context, args []string) error {
	if len(args) == 3 && args[0] == "_supervise" {
		return process.Supervise(args[1], args[2])
	}
	if len(args) == 3 && args[0] == "_exec" {
		return process.Execute(args[1], args[2])
	}
	if len(args) == 1 && args[0] == "manifest" {
		m, err := storage.Manifest()
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(m)
	}
	cfg, err := Load(os.Getenv("TREE_RUNTIME_CONFIG"))
	if err != nil {
		return err
	}
	if len(args) == 1 && (args[0] == "container-serve" || args[0] == "container-migrate") {
		return containerRuntime(ctx, cfg, args[0] == "container-migrate")
	}
	if len(args) == 1 && args[0] == "migrate" {
		return storage.Migrate(ctx, cfg.DatabaseURL)
	}
	if len(args) == 1 && args[0] == "collect" {
		return collect(ctx, cfg)
	}
	if len(args) != 0 {
		return errors.New("usage: tree-eclass [migrate|collect]")
	}
	return Serve(ctx, cfg)
}
