#!/usr/bin/env python3
"""Compliance gate for hand-written source files.

Usage: python3 scripts/quality/audit_limits.py [path...]   (default: all tracked and unignored source files)
Exit 0 = compliant, 1 = violations found.

Python and Go functions are checked with their language parsers. Frontend
functions are checked by Oxlint. All hand-written source files share the
400-line file and 120-character line limits. Go's embedded literal contents are
data rather than source lines, so the Go checker excludes them from line and
function-span measurements.
"""

import ast
import os
import subprocess
import sys
from pathlib import Path

FILE_LIMIT = 400
FN_LIMIT = 50
LINE_LIMIT = 120
SOURCE_SUFFIXES = {
    ".asm",
    ".c",
    ".cc",
    ".clj",
    ".cpp",
    ".cs",
    ".css",
    ".dart",
    ".ex",
    ".exs",
    ".fs",
    ".go",
    ".groovy",
    ".h",
    ".hh",
    ".hpp",
    ".hs",
    ".java",
    ".js",
    ".jsx",
    ".jl",
    ".kt",
    ".less",
    ".lua",
    ".m",
    ".mm",
    ".php",
    ".pl",
    ".ps1",
    ".py",
    ".rb",
    ".rs",
    ".scala",
    ".scss",
    ".sh",
    ".sql",
    ".swift",
    ".tcl",
    ".ts",
    ".tsx",
    ".vb",
    ".vue",
    ".html",
    ".htm",
    ".xhtml",
    ".zsh",
}
SPECIAL_FILES = {"tree"}
EXCLUDED_PARTS = {"dist", "node_modules", "vendor"}
EXCLUDED_PREFIXES = ("frontend/tools/oxlint/anti-slop/",)
IMMUTABLE_MIGRATION_PREFIXES = (
    "backend/internal/infrastructure/storage/migrations/",
    "backend/internal/infrastructure/rdbms/postgres_migrations/",
    "backend/internal/infrastructure/rdbms/sqlite_migrations/",
)


def source_files():
    out = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [
        Path(path)
        for path in out.stdout.splitlines()
        if Path(path).suffix in SOURCE_SUFFIXES or Path(path).name in SPECIAL_FILES
    ]


def excluded(path, src):
    text_path = os.fspath(path)
    return (
        any(part in EXCLUDED_PARTS for part in path.parts)
        or any(text_path.startswith(prefix) for prefix in EXCLUDED_PREFIXES)
        or b"Code generated" in src[:500]
    )


def audit_file(path):
    violations = []
    file_path = Path(path)
    if not file_path.is_file():
        return violations
    src = file_path.read_bytes()
    if excluded(file_path, src):
        return violations
    if file_path.suffix == ".go":
        return violations
    text = src.decode("utf-8")
    lines = text.splitlines()
    nlines = src.count(b"\n")
    immutable_migration = any(os.fspath(file_path).startswith(prefix) for prefix in IMMUTABLE_MIGRATION_PREFIXES)
    if nlines > FILE_LIMIT and not immutable_migration:
        violations.append(f"  FILE {nlines} lines > {FILE_LIMIT}: {path}")
    violations.extend(
        f"  LINE {len(line)} characters > {LINE_LIMIT}: {path}:{line_no}"
        for line_no, line in enumerate(lines, 1)
        if len(line) > LINE_LIMIT and not immutable_migration
    )
    if file_path.suffix == ".py":
        try:
            tree = ast.parse(text)
        except SyntaxError as e:
            violations.append(f"  SYNTAX ERROR {path}: {e}")
            return violations
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                n = (node.end_lineno or node.lineno) - node.lineno + 1
                if n > FN_LIMIT:
                    violations.append(f"  FN {n} lines > {FN_LIMIT}: {node.name} @ {path}:{node.lineno}")
    return violations


def main():
    if len(sys.argv) > 1:
        files = []
        for path in sys.argv[1:]:
            if os.path.isdir(path):
                for parent, _, names in os.walk(path):
                    files.extend(
                        Path(os.path.join(parent, name)) for name in names if Path(name).suffix in SOURCE_SUFFIXES
                    )
            else:
                files.append(Path(path))
    else:
        files = source_files()
    bad = 0
    for f in sorted(files):
        v = audit_file(f)
        if v:
            bad += 1
            print("\n".join(v))
    go_files = [path for path in files if path.suffix == ".go"]
    if go_files:
        root = Path(__file__).resolve().parents[2]
        backend = root / "backend"
        args = ["go", "-C", str(backend), "run", "../scripts/quality/audit_go_limits.go"]
        args.append("--")
        args.extend(os.path.relpath(root / path, backend) for path in go_files)
        result = subprocess.run(args, cwd=root)
        bad += result.returncode != 0
    print(f"\n{'FAIL' if bad else 'PASS'}: {len(files)} files audited, {bad} violating")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
