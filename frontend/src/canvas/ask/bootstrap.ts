import { useEffect, useState } from 'react';
import { ApiError } from '@/lib/api';
import { loadAskBootstrap } from '../app/loaders';
import { friendlyError, type AskBootstrap } from './model';

export interface AskLoadError {
  message: string;
  status?: number;
}

export function useAskBootstrap(initialData: AskBootstrap | null, query: string) {
  const [bootstrap, setBootstrap] = useState(initialData);
  const [error, setError] = useState<AskLoadError | null>(null);
  useEffect(() => {
    if (initialData) return;
    const controller = new AbortController();
    loadAskBootstrap(query, controller.signal)
      .then((data) => {
        if (!controller.signal.aborted) setBootstrap(data);
      })
      .catch((failure) => {
        if (!controller.signal.aborted)
          setError({
            message: friendlyError(failure),
            status: failure instanceof ApiError ? failure.status : undefined,
          });
      });
    return () => controller.abort();
  }, [initialData, query]);
  return { bootstrap, error };
}
