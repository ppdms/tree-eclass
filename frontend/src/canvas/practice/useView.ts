import { useCallback, useEffect, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { requestJson } from '../app/api';
import type { CoursePracticeView } from './model';
import { isStateUpdater } from './stateAction';

interface PracticeViewState {
  courseId: string | number;
  view: CoursePracticeView | null;
  error: string | null;
  generation: number;
  version: number;
}

export interface PracticeViewResource {
  view: CoursePracticeView | null;
  setView: Dispatch<SetStateAction<CoursePracticeView | null>>;
  error: string | null;
  generation: number;
  current: boolean;
}

export function usePracticeView(courseId: string | number, version: number): PracticeViewResource {
  const [state, setState] = useState<PracticeViewState>({
    courseId,
    view: null,
    error: null,
    generation: 0,
    version: -1,
  });
  const setView = useCallback((action: SetStateAction<CoursePracticeView | null>) => {
    setState((current) => ({
      ...current,
      view: isStateUpdater(action) ? action(current.view) : action,
    }));
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    setState((current) => ({
      ...current,
      courseId,
      view: current.courseId === courseId ? current.view : null,
      error: null,
    }));
    void requestJson<CoursePracticeView>(`/api/study/practice?course_id=${encodeURIComponent(courseId)}`, {
      signal: controller.signal,
    }).then(
      (view) => {
        if (!controller.signal.aborted) {
          setState((current) => ({ courseId, view, error: null, generation: current.generation + 1, version }));
        }
      },
      (failure) => {
        if (!controller.signal.aborted) {
          setState((current) => ({
            ...current,
            error: failure instanceof Error ? failure.message : 'The practice queue could not be loaded.',
          }));
        }
      },
    );
    return () => controller.abort();
  }, [courseId, version]);
  return {
    view: state.courseId === courseId ? state.view : null,
    setView,
    error: state.courseId === courseId ? state.error : null,
    generation: state.generation,
    current: state.courseId === courseId && state.version === version && state.generation > 0,
  };
}
