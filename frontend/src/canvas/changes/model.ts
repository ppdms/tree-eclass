import type { ChangeItem } from '@/lib/types';
import { fileHref as logicalFileHref } from '../courseFiles/helpers';

export type ChangeKind = 'added' | 'deleted' | 'modified' | 'mixed' | 'unchanged';

interface DiffFile {
  identity: string;
  name: string;
  path: string;
  type: string;
  redirect: string | null;
  diff: string | null;
}

interface Directory {
  name: string;
  path: string;
  type: string;
  children: Map<string, Directory>;
  files: DiffFile[];
}

export interface ChangeRow {
  identity: string;
  directory: boolean;
  name: string;
  path: string;
  type: string;
  prefix: string;
  kind: ChangeKind;
  href: string | null;
  diffHref: string | null;
}

export function changeKind(type: string): ChangeKind {
  if (type.includes('added')) return 'added';
  if (type.includes('deleted')) return 'deleted';
  if (type.includes('modified')) return 'modified';
  return type === 'mixed' ? 'mixed' : 'unchanged';
}

function fileIdentity(change: ChangeItem): string {
  return JSON.stringify([
    'file',
    change.id ?? null,
    change.change_type ?? null,
    change.type ?? null,
    change.file_path ?? null,
    change.path ?? null,
    change.name ?? null,
    change.display_name ?? null,
    change.redirect_url ?? null,
    change.diff_webdav_path ?? null,
  ]);
}

function insertChange(root: Directory, change: ChangeItem): void {
  const path = change.file_path || change.path || change.name || '';
  const parts = path.split('/').filter(Boolean);
  const type = change.change_type || change.type || '';
  let current = root;
  for (let index = 0; index < parts.length; index += 1) {
    const part = parts[index]!;
    const last = index === parts.length - 1;
    if (last && !type.includes('directory')) {
      current.files.push({
        identity: fileIdentity(change),
        name: change.display_name || part,
        path,
        type,
        redirect: change.redirect_url || null,
        diff: change.diff_webdav_path || null,
      });
      return;
    }
    let child = current.children.get(part);
    if (!child) {
      child = {
        name: part,
        path: parts.slice(0, index + 1).join('/'),
        type: '',
        children: new Map(),
        files: [],
      };
      current.children.set(part, child);
    }
    if (last) child.type = type;
    current = child;
  }
}

function inferDirectory(node: Directory): ChangeKind {
  const kinds = new Set<ChangeKind>();
  node.files.forEach((file) => kinds.add(changeKind(file.type)));
  node.children.forEach((child) => kinds.add(inferDirectory(child)));
  kinds.delete('unchanged');
  if (!node.type) {
    const kind = kinds.size === 1 ? kinds.values().next().value : 'mixed';
    node.type = kind && kind !== 'mixed' ? `${kind}_directory` : 'mixed';
  }
  return changeKind(node.type);
}

function fileHref(file: DiffFile, folder?: string): string | null {
  if (file.redirect) return file.redirect;
  if (file.type !== 'added_file' && file.type !== 'modified_file') return null;
  if (!folder || !file.path) return null;
  const prefix = folder.replace(/^\/+|\/+$/g, '');
  const path = file.path.split('/').filter(Boolean).join('/');
  return logicalFileHref(`${prefix}/${path}`);
}

function sortedEntries(node: Directory): (Directory | DiffFile)[] {
  const directories = [...node.children.values()].sort((a, b) => a.name.localeCompare(b.name));
  const files = [...node.files].sort((a, b) => a.name.localeCompare(b.name));
  return [...directories, ...files];
}

function collectRows(node: Directory, prefix: string, root: boolean, rows: ChangeRow[], folder?: string): void {
  const entries = sortedEntries(node);
  for (const [index, entry] of entries.entries()) {
    const last = index === entries.length - 1;
    const rowPrefix = root ? '' : prefix + (last ? '└── ' : '├── ');
    const base = { name: entry.name, path: entry.path, type: entry.type, prefix: rowPrefix };
    if ('children' in entry) {
      rows.push({
        ...base,
        identity: JSON.stringify(['directory', entry.path, entry.type]),
        directory: true,
        kind: changeKind(entry.type),
        href: null,
        diffHref: null,
      });
      collectRows(entry, prefix + (last ? '    ' : '│   '), false, rows, folder);
    } else {
      rows.push({
        ...base,
        identity: entry.identity,
        directory: false,
        kind: changeKind(entry.type),
        href: fileHref(entry, folder),
        diffHref: entry.type === 'modified_file' && entry.diff ? logicalFileHref(entry.diff) : null,
      });
    }
  }
}

export function buildChangeRows(changes: ChangeItem[], folder?: string): ChangeRow[] {
  const root: Directory = { name: '', path: '', type: '', children: new Map(), files: [] };
  changes.forEach((change) => insertChange(root, change));
  inferDirectory(root);
  const rows: ChangeRow[] = [];
  collectRows(root, '', true, rows, folder);
  const occurrences = new Map<string, number>();
  return rows.map((row) => {
    const occurrence = occurrences.get(row.identity) || 0;
    occurrences.set(row.identity, occurrence + 1);
    return { ...row, identity: JSON.stringify([row.identity, occurrence]) };
  });
}

const KIND_LABELS = {
  added_file: 'Added file',
  added_directory: 'Added folder',
  modified_file: 'Modified file',
  modified_directory: 'Modified folder',
  modified: 'Modified file',
  deleted_file: 'Deleted file',
  deleted_directory: 'Deleted folder',
  deleted: 'Deleted file',
  mixed: 'Mixed changes',
  unchanged: 'Unchanged',
};

function isKnownChangeType(type: string): type is keyof typeof KIND_LABELS {
  return Object.hasOwn(KIND_LABELS, type);
}

export function changeLabel(type: string): string {
  return isKnownChangeType(type) ? KIND_LABELS[type] : type.replaceAll('_', ' ');
}

export function changeCounts(changes: ChangeItem[]): [string, number][] {
  const counts = new Map<string, number>();
  for (const change of changes) {
    const type = change.change_type || change.type || 'modified';
    counts.set(type, (counts.get(type) || 0) + 1);
  }
  return [...counts];
}

export function changeScope(courseId: string | number, changeNo?: string): string {
  return encodeURIComponent(JSON.stringify([courseId, changeNo ?? null]));
}
