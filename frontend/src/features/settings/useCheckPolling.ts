import * as React from 'react';
import { fetchJson } from '@/lib/api';
import type { CheckStatusPayload } from './types';

function watchStatus(load: () => Promise<CheckStatusPayload | null>, cancel: () => void) {
  let stopped = false;
  let failures = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const poll = async () => {
    clearTimeout(timer);
    if (stopped || document.visibilityState !== 'visible') {
      cancel();
      return;
    }
    const result = await load();
    failures = result ? 0 : Math.min(4, failures + 1);
    if (!stopped && document.visibilityState === 'visible') {
      clearTimeout(timer);
      timer = setTimeout(poll, 5000 * 2 ** failures);
    }
  };
  void poll();
  document.addEventListener('visibilitychange', poll);
  return () => {
    stopped = true;
    clearTimeout(timer);
    cancel();
    document.removeEventListener('visibilitychange', poll);
  };
}

export function useCheckPolling() {
  const [status, setStatus] = React.useState<CheckStatusPayload | null>(null);
  const [pollingError, setError] = React.useState<string | null>(null);
  const pending = React.useRef<Promise<CheckStatusPayload | null> | null>(null);
  const controller = React.useRef<AbortController | null>(null);
  const load = React.useCallback(() => {
    if (pending.current) return pending.current;
    if (document.visibilityState !== 'visible') return Promise.resolve(null);
    const current = new AbortController();
    controller.current = current;
    pending.current = fetchJson<CheckStatusPayload>('api/check-status', { signal: current.signal })
      .then((value) => {
        if (!current.signal.aborted) {
          setStatus(value);
          setError(null);
        }
        return value;
      })
      .catch(() => {
        if (!current.signal.aborted) setError('Could not refresh course-check status.');
        return null;
      })
      .finally(() => {
        pending.current = null;
      });
    return pending.current;
  }, []);
  React.useEffect(() => watchStatus(load, () => controller.current?.abort()), [load]);
  return { status, setStatus, pollingError, load };
}
