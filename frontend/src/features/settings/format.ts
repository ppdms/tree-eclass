import { formatDateTime as formatDeterministicDateTime } from '@/lib/format';

export function formatDateTime(value: string | number | Date | null | undefined): string {
  if (!value) return '';
  return formatDeterministicDateTime(value) || String(value);
}

export function isSuccessfulResult(value: string | null | undefined): boolean {
  return value === 'ok' || value === 'success';
}
