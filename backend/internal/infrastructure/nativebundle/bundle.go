package nativebundle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"

	"tree-eclass/internal/domain/platform"
)

func Bundle(ctx context.Context, root string, executables map[string]string) error {
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		return errors.New("native helper bundling requires macOS ARM64")
	}
	g := &graph{root: root, nodes: map[string]*node{}}
	for _, dir := range []string{"bin", "lib", "share"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			return err
		}
	}
	nodes, err := stageExecutables(g, root, executables)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if err := platform.CloneFile(n.source, n.target); err != nil {
			return err
		}
		if err := os.Chmod(n.target, 0700); err != nil {
			return err
		}
	}
	for _, n := range nodes {
		changed, err := g.resources(n)
		if err != nil {
			return err
		}
		if err = relocate(ctx, n, changed); err != nil {
			return err
		}
	}
	if err := fontConfiguration(root); err != nil {
		return err
	}
	return Verify(root)
}

// stageExecutables validates and registers every requested executable, then
// returns the dependency-graph nodes ordered by target path.
func stageExecutables(g *graph, root string, executables map[string]string) ([]*node, error) {
	names := []string{}
	for name := range executables {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if filepath.Base(name) != name {
			return nil, errors.New("invalid helper executable name")
		}
		source, err := filepath.EvalSymlinks(executables[name])
		if err != nil {
			return nil, err
		}
		if _, err = g.add(source, filepath.Join(root, "bin", name), source, nil); err != nil {
			return nil, err
		}
	}
	nodes := []*node{}
	for _, n := range g.nodes {
		nodes = append(nodes, n)
	}
	slices.SortFunc(nodes, func(a, b *node) int {
		if a.target < b.target {
			return -1
		}
		if a.target > b.target {
			return 1
		}
		return 0
	})
	return nodes, nil
}

func relocate(ctx context.Context, n *node, changed bool) error {
	args := []string{}
	names := []string{}
	for name := range n.deps {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		rel, err := filepath.Rel(filepath.Dir(n.target), n.deps[name].target)
		if err != nil {
			return err
		}
		args = append(args, "-change", name, "@loader_path/"+rel)
	}
	if n.dylib {
		args = append(args, "-id", "@rpath/"+filepath.Base(n.target))
	}
	for _, path := range n.rpaths {
		args = append(args, "-delete_rpath", path)
	}
	if len(args) > 0 {
		args = append(args, n.target)
		if out, err := exec.CommandContext(ctx, "/usr/bin/install_name_tool", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("relocate %s: %w: %s", filepath.Base(n.target), err, out)
		}
		changed = true
	}
	if changed {
		if out, err := exec.CommandContext(ctx, "/usr/bin/codesign", "--force", "--sign", "-", n.target).CombinedOutput(); err != nil {
			return fmt.Errorf("sign %s: %w: %s", filepath.Base(n.target), err, out)
		}
	}
	return nil
}
