import { useCallback, useEffect, useRef, useState } from 'react';
import type { StudyPayload } from '@/lib/types';
import { requestJson } from '../app/api';
import { studyPath } from './model';

export function useStudyPlan(courseId: string | null, initialData: StudyPayload) {
  const [data, setData] = useState(initialData);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const controller = useRef<AbortController | null>(null);
  const load = useCallback(async () => {
    controller.current?.abort();
    const current = new AbortController();
    controller.current = current;
    setError(null);
    setLoading(true);
    try {
      const next = await requestJson<StudyPayload>(studyPath(courseId), { signal: current.signal });
      if (!current.signal.aborted) setData(next);
    } catch (err) {
      if (!current.signal.aborted) setError(err instanceof Error ? err.message : 'Network connection issue');
    } finally {
      if (!current.signal.aborted) setLoading(false);
    }
  }, [courseId]);
  useEffect(() => () => controller.current?.abort(), []);
  useEffect(() => {
    setData(initialData);
  }, [initialData]);
  return { data, error, loading, load };
}
