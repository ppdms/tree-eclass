import { useEffect, useReducer, useState } from 'react';
import type { Exercise } from '@/lib/types';
import { requestJson } from '../app/api';
import { exercisePath } from './model';

interface DetailPayload {
  exercise: Exercise;
}

export function useExerciseDetails(exercise: Exercise, open: boolean) {
  const path = exercisePath(exercise);
  const [attempt, retry] = useReducer((value: number) => value + 1, 0);
  const key = JSON.stringify([path, exercise.fetched_at, attempt]);
  const [cached, setCached] = useState<{ key: string; value: DetailPayload } | null>(null);
  const [failed, setFailed] = useState<{ key: string; message: string } | null>(null);
  const data = cached?.key === key ? cached.value : null;
  useEffect(() => {
    if (!open || data) return;
    const controller = new AbortController();
    setFailed(null);
    requestJson<DetailPayload>(path, { signal: controller.signal })
      .then((value) => {
        if (!controller.signal.aborted) setCached({ key, value });
      })
      .catch((failure) => {
        const error: unknown = failure;
        if (!controller.signal.aborted) {
          const message = error instanceof Error ? error.message : 'Network connection issue.';
          setFailed({ key, message });
        }
      });
    return () => controller.abort();
  }, [path, key, open, data]);
  return { data, error: failed?.key === key ? failed.message : null, retry };
}
