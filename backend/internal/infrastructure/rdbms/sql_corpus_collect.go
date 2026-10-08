package rdbms

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// collectStringConsts maps package-level string constant names to values so
// `+`-built queries (SELECT ... + Predicate + ORDER BY) resolve statically.
func collectStringConsts(t *testing.T, root string) map[string]string {
	t.Helper()
	consts := map[string]string{}
	filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil
		}
		for _, decl := range file.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok || general.Tok != token.CONST {
				continue
			}
			for _, spec := range general.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
					continue
				}
				lit, ok := value.Values[0].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				consts[value.Names[0].Name] = literalValue(lit.Value)
			}
		}
		return nil
	})
	return consts
}

// joinConcat flattens a `+` chain to its string if every leaf is a literal
// or a known package constant. Anything else (Sprintf, variables) fails.
func joinConcat(expr *ast.BinaryExpr, consts map[string]string) (string, bool) {
	var parts []string
	var flatten func(node ast.Node) bool
	flatten = func(node ast.Node) bool {
		switch leaf := node.(type) {
		case *ast.BasicLit:
			if leaf.Kind != token.STRING {
				return false
			}
			parts = append(parts, literalValue(leaf.Value))
			return true
		case *ast.Ident:
			value, ok := consts[leaf.Name]
			if !ok {
				return false
			}
			parts = append(parts, value)
			return true
		case *ast.BinaryExpr:
			if leaf.Op != token.ADD {
				return false
			}
			return flatten(leaf.X) && flatten(leaf.Y)
		}
		return false
	}
	if !flatten(expr) {
		return "", false
	}
	return strings.Join(parts, ""), true
}

func literalValue(raw string) string {
	if strings.HasPrefix(raw, "`") {
		return raw[1 : len(raw)-1]
	}
	unquoted, err := strconv.Unquote(raw)
	if err != nil {
		return ""
	}
	return unquoted
}
