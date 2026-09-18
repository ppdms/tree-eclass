import { formatDateTime } from '@/lib/format';

export function formatFileSize(value: number | null | undefined): string {
  if (value == null) return '';
  let size = Number(value);
  for (const unit of ['B', 'KB', 'MB', 'GB']) {
    if (size < 1024 || unit === 'GB') {
      return unit === 'B' ? `${Math.round(size)} ${unit}` : `${size.toFixed(1)} ${unit}`;
    }
    size /= 1024;
  }
  return '';
}

export function formatTimestamp(timestamp: string | undefined): string {
  return formatDateTime(timestamp) || String(timestamp);
}

export function relativePath(localPath: string | undefined, webdavFolder: string): string {
  if (!localPath || !webdavFolder) return '';
  const prefix = `${webdavFolder.replace(/\/+$/, '')}/`;
  return localPath.startsWith(prefix) ? localPath.slice(prefix.length) : localPath;
}

export function fileHref(path: string): string {
  if (!path) return '#';
  const normalized = String(path).startsWith('/') ? String(path) : `/${path}`;
  return `/files${encodeURI(normalized)}`;
}
