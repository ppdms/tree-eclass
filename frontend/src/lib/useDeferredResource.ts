import * as React from 'react';
import { fetchJson } from './api';

/** One response per mounted disclosure; closed requests are cancelled. */
export function useDeferredResource<T>(url: string, enabled: boolean) {
  const [attempt, retry] = React.useReducer((value: number) => value + 1, 0);
  const key = `${url}:${attempt}`;
  const [cached, setCached] = React.useState<{ key: string; value: T } | null>(null);
  const [failed, setFailed] = React.useState<string | null>(null);
  const data = cached?.key === key ? cached.value : null;
  const update = React.useCallback((value: T) => setCached({ key, value }), [key]);
  React.useEffect(() => {
    if (!enabled || data !== null) return;
    const controller = new AbortController();
    setFailed(null);
    fetchJson<T>(url, { signal: controller.signal })
      .then((value) => {
        if (!controller.signal.aborted) update(value);
      })
      .catch(() => {
        if (!controller.signal.aborted) setFailed(key);
      });
    return () => controller.abort();
  }, [url, key, enabled, data, update]);
  return { data, error: failed === key, retry, update };
}
