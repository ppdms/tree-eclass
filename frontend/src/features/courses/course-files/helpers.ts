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
