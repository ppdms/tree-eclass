package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"strings"
	"unicode/utf8"
)

const (
	fileLimit = 400
	fnLimit   = 50
	lineLimit = 120
)

type literalRange struct {
	start int
	end   int
}

func main() {
	paths := os.Args[1:]
	if len(paths) > 0 && paths[0] == "--" {
		paths = paths[1:]
	}
	violations := 0
	for _, path := range paths {
		violations += audit(path)
	}
	if violations > 0 {
		os.Exit(1)
	}
}

func audit(path string) int {
	src, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("  READ ERROR %s: %v\n", path, err)
		return 1
	}
	formatted, err := format.Source(src)
	if err != nil {
		fmt.Printf("  FORMAT ERROR %s: %v\n", path, err)
		return 1
	}
	violations := 0
	if !bytes.Equal(src, formatted) {
		fmt.Printf("  FORMAT %s is not gofmt-formatted\n", path)
		violations++
	}
	if bytes.Contains(src[:min(len(src), 500)], []byte("Code generated")) {
		return violations
	}
	lines := strings.Split(string(src), "\n")
	if len(lines) > 1 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		return violations + 1
	}
	literalRanges := stringLiteralRanges(path, src)
	tableLines := flatTableLines(file, fset)
	violations += auditLineLimits(path, src, lines, literalRanges)
	violations += auditFunctionLimits(path, fset, file, literalRanges, tableLines)
	return violations + auditFileLimit(path, len(lines))
}

func auditFileLimit(path string, lines int) int {
	if lines > fileLimit {
		fmt.Printf("  FILE %d lines > %d: %s\n", lines, fileLimit, path)
		return 1
	}
	return 0
}

func auditLineLimits(path string, src []byte, lines []string, literalRanges map[int][]literalRange) int {
	violations := 0
	for lineNo, line := range lines {
		length := utf8.RuneCountInString(line)
		for _, literal := range literalRanges[lineNo+1] {
			length -= utf8.RuneCount(src[literal.start:literal.end])
		}
		if length > lineLimit {
			fmt.Printf("  LINE %d characters > %d: %s:%d\n", length, lineLimit, path, lineNo+1)
			violations++
		}
	}
	return violations
}

func auditFunctionLimits(
	path string,
	fset *token.FileSet,
	file *ast.File,
	literalRanges map[int][]literalRange,
	tableLines map[int]bool,
) int {
	violations := 0
	ast.Inspect(file, func(node ast.Node) bool {
		var name string
		switch fn := node.(type) {
		case *ast.FuncDecl:
			name = fn.Name.Name
		case *ast.FuncLit:
			name = "<function literal>"
		default:
			return true
		}
		start := fset.Position(node.Pos()).Line
		end := fset.Position(node.End()).Line
		size := end - start + 1
		for line := start + 1; line < end; line++ {
			if len(literalRanges[line]) > 0 || tableLines[line] {
				size--
			}
		}
		if size > fnLimit {
			fmt.Printf("  FN %d lines > %d: %s @ %s:%d\n", size, fnLimit, name, path, start)
			violations++
		}
		return true
	})
	return violations
}

func flatTableLines(file *ast.File, fset *token.FileSet) map[int]bool {
	lines := make(map[int]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || !flatComposite(literal) {
			return true
		}
		start := fset.Position(literal.Pos()).Line
		end := fset.Position(literal.End()).Line
		for line := start + 1; line < end; line++ {
			lines[line] = true
		}
		return true
	})
	return lines
}

func flatComposite(literal *ast.CompositeLit) bool {
	flat := true
	ast.Inspect(literal, func(node ast.Node) bool {
		switch node.(type) {
		case *ast.CallExpr, *ast.FuncLit, *ast.FuncType, *ast.SendStmt, *ast.GoStmt, *ast.DeferStmt:
			flat = false
			return false
		}
		return flat
	})
	return flat
}

func stringLiteralRanges(path string, src []byte) map[int][]literalRange {
	fset := token.NewFileSet()
	file := fset.AddFile(path, fset.Base(), len(src))
	var scan scanner.Scanner
	scan.Init(file, src, nil, scanner.ScanComments)
	ranges := make(map[int][]literalRange)
	lineStarts := []int{0}
	for offset, value := range src {
		if value == '\n' && offset+1 < len(src) {
			lineStarts = append(lineStarts, offset+1)
		}
	}
	for {
		pos, tok, literal := scan.Scan()
		if tok == token.EOF {
			return ranges
		}
		if tok != token.STRING {
			continue
		}
		startOffset := file.Offset(pos)
		endOffset := startOffset + len(literal)
		startLine := file.Position(pos).Line
		endLine := file.Position(file.Pos(endOffset)).Line
		for line := startLine; line <= endLine; line++ {
			lineStart := lineStarts[line-1]
			lineEnd := len(src)
			if line < len(lineStarts) {
				lineEnd = lineStarts[line] - 1
			}
			start := max(startOffset, lineStart)
			end := min(endOffset, lineEnd)
			if start < end {
				ranges[line] = append(ranges[line], literalRange{start, end})
			}
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
