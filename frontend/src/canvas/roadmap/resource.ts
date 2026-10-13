import { useEffect, useReducer, useRef, useState } from 'react';
import { requestJson } from '../app/api';

interface ResourceState<T> {
  url: string;
  data: T | null;
  error: Error | null;
  pending: boolean;
  generation: number;
}

export function useRoadmapResource<T>(url: string, enabled: boolean, version = 0) {
  const [attempt, retry] = useReducer((value: number) => value + 1, 0);
  const [state, setState] = useState<ResourceState<T>>({
    url,
    data: null,
    error: null,
    pending: enabled,
    generation: 0,
  });
  const cache = useRef<{ url: string; token: string; data: T } | null>(null);
  useEffect(() => {
    if (!enabled) return;
    const token = `${version}:${attempt}`;
    if (cache.current?.url === url && cache.current.token === token) return;
    const controller = new AbortController();
    setState((previous) => ({
      url,
      data: previous.url === url ? previous.data : null,
      error: null,
      pending: true,
      generation: previous.generation,
    }));
    void requestJson<T>(url, { signal: controller.signal }).then(
      (data) => {
        if (controller.signal.aborted) return;
        cache.current = { url, token, data };
        setState((previous) => ({ url, data, error: null, pending: false, generation: previous.generation + 1 }));
      },
      (error) => {
        if (controller.signal.aborted) return;
        const failure = error instanceof Error ? error : new Error(String(error));
        setState((previous) => ({ ...previous, error: failure, pending: false }));
      },
    );
    return () => controller.abort();
  }, [url, enabled, version, attempt]);
  return {
    ...state,
    data: state.url === url ? state.data : null,
    error: state.url === url ? state.error : null,
    retry,
  };
}

export function isRevisionConflict(error: Error | null): boolean {
  return Boolean(error && 'status' in error && error.status === 409);
}
