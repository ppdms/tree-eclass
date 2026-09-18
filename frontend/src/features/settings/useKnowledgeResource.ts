import * as React from 'react';
import { fetchJson } from '@/lib/api';
import type { KnowledgeStatusPayload } from './types';

function startPolling(path: string, enabled: boolean, publish: (data: KnowledgeStatusPayload) => void) {
  let disposed = false;
  let running = false;
  let failures = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const controller = new AbortController();
  const load = async () => {
    if (disposed || running || !enabled || document.visibilityState !== 'visible') return;
    running = true;
    clearTimeout(timer);
    try {
      const data = await fetchJson<KnowledgeStatusPayload>(path, { signal: controller.signal });
      if (!disposed) publish(data);
      failures = 0;
    } catch {
      failures = Math.min(failures + 1, 4);
    } finally {
      running = false;
      if (!disposed) timer = setTimeout(load, 30000 * 2 ** failures);
    }
  };
  void load();
  document.addEventListener('visibilitychange', load);
  return () => {
    disposed = true;
    controller.abort();
    clearTimeout(timer);
    document.removeEventListener('visibilitychange', load);
  };
}

export function useKnowledgeResource(path: string, enabled = true) {
  const [status, setStatus] = React.useState<KnowledgeStatusPayload | null>(null);
  const [generation, refresh] = React.useReducer((value: number) => value + 1, 0);
  React.useEffect(() => startPolling(path, enabled, setStatus), [path, enabled, generation]);
  return { status, refresh };
}
