import * as React from 'react';
import type { CourseVersion } from '@/lib/types';
import { requestJson } from '../app/api';

interface HistoryResponse {
  versions?: CourseVersion[];
  deleted?: CourseVersion[];
}

export function useHistory(url: string, deleted: boolean) {
  const [open, setOpen] = React.useState(false);
  const [entries, setEntries] = React.useState<CourseVersion[]>([]);
  const [error, setError] = React.useState<string | null>(null);
  const [pending, setPending] = React.useState(false);
  const active = React.useRef<AbortController | null>(null);
  React.useEffect(() => {
    setOpen(false);
    setEntries([]);
    setError(null);
    setPending(false);
    return () => active.current?.abort();
  }, [url]);
  const toggle = async () => {
    if (open) {
      setOpen(false);
      return;
    }
    if (active.current && !active.current.signal.aborted) return;
    const controller = new AbortController();
    active.current = controller;
    setPending(true);
    setError(null);
    try {
      const data = await requestJson<HistoryResponse>(url, { signal: controller.signal });
      if (!controller.signal.aborted) setEntries((deleted ? data.deleted : data.versions) || []);
    } catch (cause) {
      if (!controller.signal.aborted) {
        const fallback = deleted ? 'Failed to load deleted files' : 'Failed to load versions';
        setError(cause instanceof Error ? cause.message : fallback);
      }
    } finally {
      if (!controller.signal.aborted) {
        active.current = null;
        setPending(false);
        setOpen(true);
      }
    }
  };
  return { open, entries, error, pending, toggle };
}
