import { useCallback, useEffect, useState } from 'react';
import type { CourseSummary } from '@/lib/types';
import { requestJson } from '../app/api';
import { publishCourseOrder } from '../app/events';
import {
  confirmSavedOrder,
  createOrderSession,
  refreshOrderSession,
  type CoursesCatalogData,
  type OrderSession,
} from './orderState';

type SaveState = 'idle' | 'saving' | 'saved' | 'error';

export interface CourseOrder {
  ordered: CourseSummary[];
  previewMove: (sourceId: string, targetId: string) => void;
  beginDrag: () => void;
  commit: () => void;
  cancel: () => void;
  saveState: SaveState;
}

export function sameOrder(first: CourseSummary[], second: CourseSummary[]) {
  return (
    first.length === second.length && first.every((course, index) => String(course.id) === String(second[index]?.id))
  );
}

export function reordered(courses: CourseSummary[], sourceId: string, targetId: string) {
  const sourceIndex = courses.findIndex((course) => String(course.id) === sourceId);
  const targetIndex = courses.findIndex((course) => String(course.id) === targetId);
  if (sourceIndex < 0 || targetIndex < 0 || sourceIndex === targetIndex) return courses;
  const next = [...courses];
  const [source] = next.splice(sourceIndex, 1);
  if (!source) return courses;
  next.splice(targetIndex, 0, source);
  return next;
}

function usePendingOrder(
  pending: CourseSummary[] | null,
  session: OrderSession,
  setPending: (courses: CourseSummary[] | null) => void,
  setSaveState: (state: SaveState) => void,
  replaceOrder: (courses: CourseSummary[]) => void,
) {
  useEffect(() => {
    if (!pending) return;
    const controller = new AbortController();
    const timer = globalThis.setTimeout(() => {
      const attempted = session.pending;
      if (!attempted) return;
      setSaveState('saving');
      void requestJson<{ status: string; course_ids: number[] }>('/api/courses/reorder', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(attempted.map(({ id }) => Number(id))),
        signal: controller.signal,
      })
        .then(() => {
          if (controller.signal.aborted) return;
          confirmSavedOrder(session, attempted);
          publishCourseOrder(session.ordered);
          replaceOrder(session.ordered);
          setPending(null);
          setSaveState('saved');
        })
        .catch(() => {
          if (controller.signal.aborted) return;
          session.pending = null;
          replaceOrder(session.saved);
          setPending(null);
          setSaveState('error');
        });
    }, 220);
    return () => {
      globalThis.clearTimeout(timer);
      controller.abort();
    };
  }, [pending, session, replaceOrder, setPending, setSaveState]);
}

function useOrderActions(
  session: OrderSession,
  replaceOrder: (courses: CourseSummary[]) => void,
  setPending: (courses: CourseSummary[] | null) => void,
  setSaveState: (state: SaveState) => void,
) {
  const previewMove = useCallback(
    (sourceId: string, targetId: string) => {
      const next = reordered(session.ordered, sourceId, targetId);
      if (next === session.ordered) return;
      replaceOrder(next);
      setSaveState('idle');
    },
    [session, replaceOrder, setSaveState],
  );
  const commit = useCallback(() => {
    session.dragging = false;
    const next = sameOrder(session.ordered, session.saved) ? null : [...session.ordered];
    session.pending = next;
    setPending(next);
  }, [session, setPending]);
  const beginDrag = useCallback(() => {
    session.dragging = true;
    session.pending = null;
    setPending(null);
    setSaveState('idle');
  }, [session, setPending, setSaveState]);
  const cancel = useCallback(() => {
    session.dragging = false;
    session.pending = null;
    setPending(null);
    replaceOrder(session.saved);
    setSaveState('idle');
  }, [session, replaceOrder, setPending, setSaveState]);
  return { previewMove, beginDrag, commit, cancel };
}

export function useCourseOrder(data: CoursesCatalogData): CourseOrder {
  const [session] = useState(() => createOrderSession(data));
  const [ordered, setOrdered] = useState(session.ordered);
  const [pending, setPending] = useState<CourseSummary[] | null>(null);
  const [saveState, setSaveState] = useState<SaveState>('idle');
  const replaceOrder = useCallback(
    (next: CourseSummary[]) => {
      session.ordered = next;
      setOrdered(next);
    },
    [session],
  );
  const actions = useOrderActions(session, replaceOrder, setPending, setSaveState);
  useEffect(() => {
    refreshOrderSession(session, data);
    replaceOrder(session.ordered);
  }, [data, session, replaceOrder]);
  usePendingOrder(pending, session, setPending, setSaveState, replaceOrder);
  return { ordered, ...actions, saveState };
}
