import { useEffect, useState } from 'react';
import { api, errorStatus, message } from './api';
import type { Context, SessionQuery } from './types';

export function useSessionContext(query: SessionQuery) {
  const [context, setContext] = useState<Context | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    setContext(null);
    setError(null);
    setPending(false);
    const load = async () => {
      try {
        const result = await api.context(query, controller.signal);
        if (controller.signal.aborted) return;
        setContext(result);
        setPending(false);
      } catch (failure) {
        if (controller.signal.aborted) return;
        if (query.documentId && errorStatus(failure) === 409) {
          setPending(true);
          timer = setTimeout(() => {
            void load();
          }, 5000);
        } else setError(message(failure));
      }
    };
    void load();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [query.identity, attempt]);
  return { context, error, pending, retry: () => setAttempt((value) => value + 1) };
}
