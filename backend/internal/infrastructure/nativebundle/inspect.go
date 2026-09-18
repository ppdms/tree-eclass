// Package nativebundle isolates macOS helper executables and their non-system
// libraries. The OS frameworks remain the platform boundary.
package nativebundle

import (
	"crypto/sha256"
	"debug/macho"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type node struct {
	source, target string
	dylib          bool
	rpaths         []string
	deps           map[string]*node
}
type graph struct {
	nodes map[string]*node
	root  string
}

func (g *graph) add(source, target, executable string, inherited []string) (*node, error) {
	canonical, err := filepath.EvalSymlinks(source)
	if err != nil {
		return nil, err
	}
	if old := g.nodes[canonical]; old != nil {
		return old, nil
	}
	file, err := macho.Open(canonical)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if file.Cpu != macho.CpuArm64 {
		return nil, errors.New("native release helper is not macOS ARM64")
	}
	if target == "" {
		sum := sha256.Sum256([]byte(canonical))
		target = filepath.Join(g.root, "lib", fmt.Sprintf("%x-%s", sum[:6], filepath.Base(canonical)))
	}
	n := &node{source: canonical, target: target, dylib: file.Type == macho.TypeDylib, deps: map[string]*node{}}
	g.nodes[canonical] = n
	paths := []string{}
	for _, load := range file.Loads {
		if rpath, ok := load.(*macho.Rpath); ok {
			n.rpaths = append(n.rpaths, rpath.Path)
			paths = append(paths, expand(rpath.Path, canonical, executable))
		}
	}
	paths = append(paths, inherited...)
	libraries, err := imports(file)
	if err != nil {
		return nil, err
	}
	for _, library := range libraries {
		if system(library) {
			continue
		}
		resolved, err := resolve(library, canonical, executable, paths)
		if err != nil {
			return nil, err
		}
		if system(resolved) {
			continue
		}
		dep, err := g.add(resolved, "", executable, paths)
		if err != nil {
			return nil, err
		}
		n.deps[library] = dep
	}
	return n, nil
}
func system(path string) bool {
	return strings.HasPrefix(path, "/usr/lib/") || strings.HasPrefix(path, "/System/Library/")
}
func expand(path, loader, executable string) string {
	path = strings.ReplaceAll(path, "@loader_path", filepath.Dir(loader))
	return strings.ReplaceAll(path, "@executable_path", filepath.Dir(executable))
}
func resolve(library, loader, executable string, paths []string) (string, error) {
	name := expand(library, loader, executable)
	candidates := []string{name}
	if strings.HasPrefix(name, "@rpath/") {
		candidates = nil
		for _, path := range paths {
			candidates = append(candidates, filepath.Join(path, strings.TrimPrefix(name, "@rpath/")))
		}
	}
	for _, candidate := range candidates {
		if !filepath.IsAbs(candidate) {
			continue
		}
		if system(candidate) {
			return candidate, nil
		}
		if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
			return filepath.EvalSymlinks(candidate)
		}
	}
	return "", fmt.Errorf("cannot resolve native dependency %s from %s", library, loader)
}

// debug/macho exposes ordinary load commands but not every weak/reexport form.
// Read their common dylib_command layout so those edges cannot escape packaging.
func imports(file *macho.File) ([]string, error) {
	var result []string
	for _, load := range file.Loads {
		raw := load.Raw()
		if len(raw) < 8 {
			return nil, errors.New("truncated Mach-O load command")
		}
		cmd := file.ByteOrder.Uint32(raw[:4])
		switch cmd {
		case 0xc, 0x80000018, 0x8000001f, 0x20, 0x80000023:
		default:
			continue
		}
		if len(raw) < 24 {
			return nil, errors.New("truncated Mach-O dependency")
		}
		offset := file.ByteOrder.Uint32(raw[8:12])
		if offset < 24 || int(offset) >= len(raw) {
			return nil, errors.New("invalid Mach-O dependency offset")
		}
		value := string(raw[offset:])
		end := strings.IndexByte(value, 0)
		if end < 0 {
			return nil, errors.New("unterminated Mach-O dependency")
		}
		result = append(result, value[:end])
	}
	return result, nil
}
