package workflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"tree-eclass/internal/domain/platform"
	"tree-eclass/internal/infrastructure/process"
	"tree-eclass/internal/services/library"
)

const usage = `tree — native laptop workflow

  setup                    Initialize private native storage and local Git remote
  controller update        Replace the installed controller while fully stopped
  up                       Start the selected stable release
  dev up                   Start an editable, disposable development session
  dev down | down          Stop everything and discard development data
  release build            Build an immutable clean committed release
  release promote          Build, activate and start the current clean commit
  release use COMMIT       Activate code on the restored stable dataset
  release rollback         Restore the previous activation checkpoint and release
  snapshot list            List protected local recovery checkpoints
  snapshot restore ID      Restore data and schema (discards later stable writes)
  credentials import       Import provider keys while stopped, before development
  mcp                      Bridge stdio MCP to the manually started native API
  status | doctor          Inspect selection, ownership and native prerequisites
  logs SERVICE             Show recent api/frontend-build/postgres/migration/collection/watcher output
  check                    Suspend, validate with synthetic data, clean and resume
  storage collect          Remove abandoned objects while stopped
  clean                    Remove owned disposable output while stopped
`

// Run executes one native controller command.
func Run(ctx context.Context, args []string) error {
	if len(args) == 4 && args[0] == "_watch" {
		return Watch(ctx, args[1], args[2], args[3])
	}
	if len(args) == 3 && args[0] == "_exec" {
		return process.Execute(args[1], args[2])
	}
	if len(args) == 3 && args[0] == "_supervise" {
		return process.Supervise(args[1], args[2])
	}
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		fmt.Print(usage)
		return nil
	}
	repo := os.Getenv("TREE_SOURCE_ROOT")
	if repo == "" {
		var err error
		repo, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	c, err := Open(repo)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.command(ctx, args)
}
func (c *Controller) command(ctx context.Context, args []string) error {
	if len(args) == 1 {
		switch args[0] {
		case "_install":
			if err := c.requireStoppedSetup(); err != nil {
				return err
			}
			if err := InstallController(c.Executable, filepath.Join(c.Root, "controller")); err != nil {
				return err
			}
			fmt.Println("Native controller installed. Startup and shutdown do not compile editable code.")
			return nil
		case "setup":
			return c.Setup(ctx)
		case "up":
			return c.Up(ctx)
		case "down":
			return c.Down()
		case "mcp":
			if err := c.configured(); err != nil {
				return err
			}
			endpoint := fmt.Sprintf("http://127.0.0.1:%d/mcp", c.Config.Ports.HTTP)
			c.Close() // Never hold the lifecycle lock for a client session.
			return library.Proxy(ctx, endpoint, library.Stdio(os.Stdin, os.Stdout))
		case "status":
			return c.Status()
		case "doctor":
			return c.Doctor(ctx)
		case "check":
			return c.Check(ctx)
		case "clean":
			return c.Clean()
		}
	}
	if len(args) == 2 {
		switch args[0] + " " + args[1] {
		case "dev up":
			return c.DevUp(ctx)
		case "dev down":
			return c.Down()
		case "release build":
			return c.ReleaseBuild(ctx)
		case "release promote":
			return c.releasePromote(ctx)
		case "release rollback":
			return c.ReleaseRollback()
		case "snapshot list":
			return c.SnapshotList()
		case "storage collect":
			return c.Collect(ctx)
		case "credentials import":
			return c.ImportProviderKeys(ctx)
		}
		if args[0] == "logs" {
			return c.Logs(args[1])
		}
	}
	if len(args) == 3 {
		switch args[0] + " " + args[1] {
		case "release use":
			return c.ReleaseUse(ctx, args[2])
		case "snapshot restore":
			return c.SnapshotRestore(args[2])
		}
	}
	return errors.New("unknown command; run ./tree help")
}

// InstallController isolates lifecycle recovery from editable application files.
func InstallController(source, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return err
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.CreateTemp(filepath.Dir(target), "controller-*")
	if err != nil {
		return err
	}
	defer os.Remove(out.Name())
	if err = out.Chmod(0700); err != nil {
		out.Close()
		return err
	}
	if _, err = io.Copy(out, in); err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(out.Name(), target); err != nil {
		return err
	}
	return platform.SyncDir(filepath.Dir(target))
}

func (c *Controller) requireStoppedSetup() error {
	if c.State.Baseline != "" || c.State.Mode != "" && c.State.Mode != "stopped" {
		return errors.New("stop the runtime with ./tree down before setup or controller update")
	}
	if _, err := os.Lstat(filepath.Join(c.Root, "activation.json")); !os.IsNotExist(err) {
		return errors.New("recover the pending activation with ./tree down before setup or controller update")
	}
	return c.stopped()
}
