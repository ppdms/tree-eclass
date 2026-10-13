import { useCallback, useEffect, useReducer, useRef, useState } from 'react';
import { requestJson } from '../app/api';
import { getActivity, subscribeActivity } from '../app/activityState';
import type { KnowledgeStatusPayload, SyncStatusPayload } from '@/features/settings/types';

export interface PollResource<T> {
  data: T | null;
  error: string | null;
  refresh(): void;
  acknowledge(update: (previous: T | null) => T): void;
}
interface PollOptions<T> {
  path: string;
  interval: number;
  enabled: boolean;
  abortHidden: boolean;
  isCurrent(): boolean;
  publish(data: T): void;
  fail(error: string | null): void;
}
export function watchResource<T>({ path, interval, enabled, abortHidden, isCurrent, publish, fail }: PollOptions<T>) {
  let disposed = false;
  let running = false;
  let failures = 0;
  let controller: AbortController | null = null;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const load = async () => {
    if (disposed || !isCurrent() || running || !enabled || !getActivity().visible) return;
    running = true;
    clearTimeout(timer);
    controller = new AbortController();
    try {
      const value = await requestJson<T>(path, { signal: controller.signal });
      if (!disposed && isCurrent() && !controller.signal.aborted) {
        publish(value);
        fail(null);
      }
      failures = 0;
    } catch {
      if (!disposed && isCurrent() && !controller.signal.aborted) {
        failures = Math.min(4, failures + 1);
        fail(
          path === '/api/check-status'
            ? 'Could not refresh course-check status.'
            : 'Could not refresh knowledge status.',
        );
      }
    } finally {
      running = false;
      if (!disposed && isCurrent() && getActivity().visible) timer = setTimeout(load, interval * 2 ** failures);
    }
  };
  const unsubscribe = subscribeActivity((event) => {
    if (event.kind !== 'visibility') return;
    clearTimeout(timer);
    if (event.visible) void load();
    else if (abortHidden) controller?.abort();
  });
  void load();
  return () => {
    disposed = true;
    clearTimeout(timer);
    controller?.abort();
    unsubscribe();
  };
}
export function usePollResource<T>(
  path: string,
  interval: number,
  enabled = true,
  abortHidden = false,
): PollResource<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [generation, reload] = useReducer((value: number) => value + 1, 0);
  const epoch = useRef(0);
  const refresh = useCallback(() => {
    // Invalidate old reads immediately, before React cleans up their effect.
    epoch.current++;
    reload();
  }, []);
  const acknowledge = useCallback(
    (update: (previous: T | null) => T) => {
      refresh();
      setData(update);
    },
    [refresh],
  );
  useEffect(() => {
    const current = epoch.current;
    return watchResource<T>({
      path,
      interval,
      enabled,
      abortHidden,
      isCurrent: () => epoch.current === current,
      publish: setData,
      fail: setError,
    });
  }, [path, interval, enabled, abortHidden, generation]);
  return { data, error, refresh, acknowledge };
}
export function useSyncStatus(): SyncStatusPayload | null {
  const [status, setStatus] = useState<SyncStatusPayload | null>(null);
  useEffect(() => {
    const controller = new AbortController();
    void requestJson<SyncStatusPayload>('/api/settings/sync-status', { signal: controller.signal })
      .then((value) => {
        if (!controller.signal.aborted) setStatus(value);
      })
      .catch(() => undefined);
    return () => controller.abort();
  }, []);
  return status;
}
export function useKnowledgeSummary(): PollResource<KnowledgeStatusPayload> {
  return usePollResource<KnowledgeStatusPayload>('/api/knowledge/summary', 30000);
}
