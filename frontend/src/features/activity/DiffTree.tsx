import * as stylex from '@stylexjs/stylex';
import { diffTreeStyles } from './diffTreeStyles';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import type { ChangeItem } from '@/lib/types';

const styles = diffTreeStyles;

// ── Tree building (port of git d04549a frontend/dist/client/script-diff.js) ──

interface DiffFile {
  name: string;
  path: string;
  changeType: string | undefined;
  redirectUrl: string | null;
  diffWebdavPath: string | null;
}

interface TreeNode {
  name: string;
  children: Record<string, TreeNode>;
  files: DiffFile[];
  changeType: string | null;
}

function newDir(name: string, changeType: string | null): TreeNode {
  return { name, children: {}, files: [], changeType: changeType || null };
}

function insertChange(root: TreeNode, change: ChangeItem): void {
  const parts = String(change.file_path || '')
    .split('/')
    .filter(Boolean);
  let current = root;
  for (let i = 0; i < parts.length; i += 1) {
    const part = parts[i]!;
    const isLast = i === parts.length - 1;
    if (!isLast) {
      // Intermediate directory: created with a null type, inferred later.
      if (!current.children[part]) current.children[part] = newDir(part, null);
      current = current.children[part]!;
    } else if (String(change.change_type || '').includes('directory')) {
      if (current.children[part]) current.children[part]!.changeType = change.change_type ?? null;
      else current.children[part] = newDir(part, change.change_type ?? null);
    } else {
      current.files.push({
        name: change.display_name || part,
        path: change.file_path || '',
        changeType: change.change_type,
        redirectUrl: change.redirect_url || null,
        diffWebdavPath: change.diff_webdav_path || null,
      });
    }
  }
}

interface ChangeFlags {
  added: boolean;
  deleted: boolean;
  modified: boolean;
}

function collectChangeFlags(node: TreeNode): ChangeFlags {
  const flags = { added: false, deleted: false, modified: false };
  for (const file of node.files) {
    const type = String(file.changeType || '');
    if (type.includes('added')) flags.added = true;
    if (type.includes('deleted')) flags.deleted = true;
    if (type.includes('modified')) flags.modified = true;
  }
  for (const child of Object.values(node.children)) {
    const childType = String(inferChangeTypes(child) || '');
    if (childType.includes('added')) flags.added = true;
    if (childType.includes('deleted')) flags.deleted = true;
    if (childType.includes('modified')) flags.modified = true;
  }
  return flags;
}

export function inferChangeTypes(node: TreeNode): string {
  const { added, deleted, modified } = collectChangeFlags(node);
  if (!node.changeType) {
    const kinds = [added, deleted, modified].filter(Boolean).length;
    if (kinds > 1) node.changeType = 'mixed';
    else if (added) node.changeType = 'added_directory';
    else if (deleted) node.changeType = 'deleted_directory';
    else if (modified) node.changeType = 'modified_directory';
  }
  return node.changeType || 'mixed';
}

export function buildDiffTree(changes: ChangeItem[]): TreeNode {
  const root = newDir('', null);
  for (const change of changes || []) insertChange(root, change);
  for (const child of Object.values(root.children)) inferChangeTypes(child);
  return root;
}

// ── Presentation helpers ──

function changeSymbol(changeType: string | null | undefined): string {
  const type = String(changeType || '');
  if (!type || type === 'unchanged') return '';
  if (type.includes('added')) return '+';
  if (type.includes('deleted')) return '\u2212';
  if (type.includes('modified')) return '~';
  if (type === 'mixed') return '\u00b1';
  return '';
}

function sortedEntries(node: TreeNode): { dir?: TreeNode; file?: DiffFile }[] {
  // Directories first, then files; each group alphabetical (old script-diff.js order).
  const dirs = Object.values(node.children).sort((a, b) => a.name.localeCompare(b.name));
  const files = [...node.files].sort((a, b) => a.name.localeCompare(b.name));
  return [...dirs.map((dir) => ({ dir })), ...files.map((file) => ({ file }))];
}

// Flatten the tree into rows with tree(1)-style connector prefixes. Each
// ancestor contributes a '│   ' / '    ' column from its last-child flag,
// then the node's own '├── ' / '└── ' connector. Root-level nodes get no
// prefix at all.
type Row = { kind: 'directory'; prefix: string; dir: TreeNode } | { kind: 'file'; prefix: string; file: DiffFile };

function collectRows(node: TreeNode, prefix: string, atRoot: boolean, rows: Row[]): void {
  const entries = sortedEntries(node);
  entries.forEach((entry, index) => {
    const isLast = index === entries.length - 1;
    const rowPrefix = atRoot ? '' : prefix + (isLast ? '└── ' : '├── ');
    const childPrefix = prefix + (isLast ? '    ' : '│   ');
    if (entry.dir) {
      rows.push({ kind: 'directory', prefix: rowPrefix, dir: entry.dir });
      collectRows(entry.dir, childPrefix, false, rows);
    } else if (entry.file) {
      rows.push({ kind: 'file', prefix: rowPrefix, file: entry.file });
    }
  });
}

function encodePathSegments(path: string): string {
  return String(path)
    .split('/')
    .filter(Boolean)
    .map((segment) => encodeURIComponent(segment))
    .join('/');
}

function webdavHref(webdavFolder: string, path: string): string {
  const folder = String(webdavFolder).replace(/^\/+|\/+$/g, '');
  return `/files/${folder}/${encodePathSegments(path)}`;
}

function fileHref(file: DiffFile, webdavFolder: string | undefined): string | null {
  if (file.redirectUrl) return file.redirectUrl;
  const linkable = file.changeType === 'added_file' || file.changeType === 'modified_file';
  if (linkable && webdavFolder && file.path) return webdavHref(webdavFolder, file.path);
  return null;
}

// ── Rows ──

function BranchPrefix({ prefix }: { prefix: string }) {
  if (!prefix) return null;
  return (
    <span {...stylex.props(styles.diffTreeBranch)} aria-hidden="true">
      {prefix}
    </span>
  );
}

function DiffTreeIcon({ kind }: { kind: 'folder' | 'file' }) {
  return (
    <span {...stylex.props(styles.diffTreeIcon)}>
      <Icon name={kind === 'folder' ? 'folder-fill' : 'file-earmark'} aria-hidden="true" />
    </span>
  );
}

function DiffTreeChangeSymbol({ symbol }: { symbol: string | null }) {
  return symbol ? <span {...stylex.props(styles.diffTreeSymbol)}>{symbol}</span> : null;
}

function changeStyleKey(changeType: string): keyof typeof styles {
  switch (changeType) {
    case 'added_file':
      return 'addedFile';
    case 'added_directory':
      return 'addedDirectory';
    case 'deleted_file':
      return 'deletedFile';
    case 'deleted_directory':
      return 'deletedDirectory';
    case 'modified_file':
      return 'modifiedFile';
    case 'modified_directory':
      return 'modifiedDirectory';
    case 'mixed':
      return 'mixed';
    default:
      return 'unchanged';
  }
}

function DirectoryRow({ row }: { row: Extract<Row, { kind: 'directory' }> }) {
  const dir = row.dir;
  const symbol = changeSymbol(dir.changeType);
  const isDeleted = Boolean(dir.changeType && dir.changeType.includes('deleted'));
  return (
    <div
      {...stylex.props(
        styles.diffTreeNode,
        styles.diffTreeDirectory,
        dir.changeType ? styles[changeStyleKey(dir.changeType)] : null,
      )}
      role="listitem"
    >
      <BranchPrefix prefix={row.prefix} />
      <DiffTreeIcon kind="folder" />
      <DiffTreeChangeSymbol symbol={symbol} />
      <span
        {...stylex.props(styles.diffTreeName, styles.diffTreeDirectoryName, isDeleted && styles.diffTreeNameDeleted)}
      >
        {dir.name}
      </span>
    </div>
  );
}

function FileRow({ row, webdavFolder }: { row: Extract<Row, { kind: 'file' }>; webdavFolder?: string }) {
  const file = row.file;
  const href = fileHref(file, webdavFolder);
  const symbol = changeSymbol(file.changeType);
  const diffPath = file.changeType === 'modified_file' ? file.diffWebdavPath : null;
  return (
    <div
      {...stylex.props(
        styles.diffTreeNode,
        styles.diffTreeFile,
        file.changeType ? styles[changeStyleKey(file.changeType)] : null,
      )}
      role="listitem"
    >
      <BranchPrefix prefix={row.prefix} />
      <DiffTreeIcon kind="file" />
      <DiffTreeChangeSymbol symbol={symbol} />
      <FileRowName file={file} href={href} />
      {diffPath ? <FileRowDiffLink path={diffPath} /> : null}
    </div>
  );
}

function FileRowName({ file, href }: { file: Extract<Row, { kind: 'file' }>['file']; href: string | null }) {
  const isDeleted = Boolean(file.changeType && file.changeType.includes('deleted'));
  return (
    <span {...stylex.props(styles.diffTreeName, isDeleted && styles.diffTreeNameDeleted)}>
      {href ? (
        <a {...stylex.props(styles.diffTreeLink)} href={href} target="_blank" rel="noopener noreferrer">
          {file.name}
        </a>
      ) : (
        file.name
      )}
    </span>
  );
}

function FileRowDiffLink({ path }: { path: string }) {
  return (
    <a
      {...stylex.props(styles.diffPdfBtn)}
      href={`/files${path}`}
      target="_blank"
      rel="noopener noreferrer"
      aria-label="Open visual diff"
      title="Open visual diff"
    >
      <Icon name="file-diff" aria-hidden="true" />
    </a>
  );
}

export function DiffTree({ changes, webdavFolder }: { changes: ChangeItem[]; webdavFolder?: string }) {
  if (!changes || changes.length === 0) {
    return <p {...stylex.props(styles.diffTreeEmpty)}>No file-level details were recorded for this change.</p>;
  }
  const rows: Row[] = [];
  collectRows(buildDiffTree(changes), '', true, rows);
  return (
    <div {...stylex.props(styles.diffTreeView, styles.diffTreeLines)} role="list">
      {rows.map((row, index) => {
        const key = `${row.kind}-${index}`;
        return row.kind === 'directory' ? (
          <DirectoryRow key={key} row={row} />
        ) : (
          <FileRow key={key} row={row} webdavFolder={webdavFolder} />
        );
      })}
    </div>
  );
}
