import { useCallback, useEffect, useRef, useState } from 'react';
import { requestJson } from '../app/api';
import type { SettingsPayload } from './shared';
import { message } from './shared';

let nextSequence = 0;
let savedSequence = 0;
const savedListeners = new Set<() => void>();

export function publishSettingsSaved(): void {
  savedSequence = ++nextSequence;
  for (const listener of savedListeners) listener();
}

export interface SettingsLoad {
  data: SettingsPayload;
  sequence: number;
}
export async function fetchSettings(signal?: AbortSignal): Promise<SettingsLoad> {
  const sequence = ++nextSequence;
  const controller = new AbortController();
  const cancel = () => controller.abort();
  signal?.addEventListener('abort', cancel, { once: true });
  if (signal?.aborted) controller.abort();
  const timer = setTimeout(cancel, 8000);
  try {
    const data = await requestJson<SettingsPayload>('/api/v1/settings', { signal: controller.signal });
    return { data, sequence };
  } catch (error) {
    if (controller.signal.aborted && !signal?.aborted) throw new Error('Settings took too long to load.');
    throw error;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', cancel);
  }
}
export interface SettingsResource {
  data: SettingsPayload;
  loading: boolean;
  error: string | null;
  refresh(): void;
}
export function useSettingsData(initial: SettingsLoad): SettingsResource {
  const [data, setData] = useState(initial.data);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const accepted = useRef(initial.sequence);
  const controller = useRef<AbortController | null>(null);
  useEffect(() => {
    if (initial.sequence < accepted.current) return;
    accepted.current = initial.sequence;
    setData(initial.data);
  }, [initial]);
  useEffect(() => () => controller.current?.abort(), []);
  const refresh = useCallback(() => {
    controller.current?.abort();
    const current = new AbortController();
    controller.current = current;
    setLoading(true);
    setError(null);
    const pending = fetchSettings(current.signal);
    accepted.current = nextSequence;
    void pending
      .then((result) => {
        if (current.signal.aborted || result.sequence < accepted.current) return;
        accepted.current = result.sequence;
        setData(result.data);
      })
      .catch((failure) => {
        if (!current.signal.aborted) setError(message(failure, 'Network connection issue'));
      })
      .finally(() => {
        if (!current.signal.aborted) setLoading(false);
      });
  }, []);
  useEffect(() => {
    savedListeners.add(refresh);
    // A save may finish between this route's loader and its subscription.
    if (initial.sequence < savedSequence) refresh();
    return () => {
      savedListeners.delete(refresh);
    };
  }, [initial.sequence, refresh]);
  return { data, loading, error, refresh };
}
