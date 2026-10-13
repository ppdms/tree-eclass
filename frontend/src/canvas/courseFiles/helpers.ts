import type { CourseFileNode, FileInsight } from '@/lib/types';
import { formatDateTime } from '@/lib/format';

export function fileIdentity(prefix: string, ...parts: unknown[]): string {
  return `${prefix}-${encodeURIComponent(JSON.stringify(parts))}`;
}

/** Encode each logical component, including literal percent signs in archive-member identities. */
export function fileHref(path: string): string {
  if (!path) return '#';
  const logical = path.startsWith('/') ? path.slice(1) : path;
  return `/files/${logical.split('/').map(encodeURIComponent).join('/')}`;
}

export function relativePath(localPath: string | undefined, webdavFolder: string): string {
  if (!localPath || !webdavFolder) return '';
  const prefix = `${webdavFolder.replace(/\/+$/, '')}/`;
  return localPath.startsWith(prefix) ? localPath.slice(prefix.length) : localPath;
}

export function formatTimestamp(timestamp: string | undefined): string {
  return formatDateTime(timestamp) || String(timestamp);
}

export function filePath(file: CourseFileNode): string {
  return file.local_path || file.url || file.name;
}

export function fileTarget(file: CourseFileNode): string {
  return file.redirect_url || (file.local_path ? fileHref(file.local_path) : file.url) || '#';
}

function matches(node: CourseFileNode, query: string): boolean {
  return [node.name, node.local_path].some((value) =>
    String(value || '')
      .toLowerCase()
      .includes(query),
  );
}

export function filterTree(node: CourseFileNode, query: string): CourseFileNode | null {
  if (!query || matches(node, query)) return node;
  const files = (node.files || []).filter((file) => matches(file, query));
  const children = (node.children || [])
    .map((child) => filterTree(child, query))
    .filter((child): child is CourseFileNode => child !== null);
  return files.length || children.length ? { ...node, files, children } : null;
}

export function guideIdentity(insight: FileInsight): string {
  return JSON.stringify([
    insight.course_id,
    insight.id,
    insight.source_hash,
    insight.enrichment_model,
    insight.enrichment_analysis_version,
    insight.enrichment_generated_at,
  ]);
}
