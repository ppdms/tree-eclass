import { useCallback, useEffect, useRef, useState, type MutableRefObject } from 'react';
import { fetchJson } from '@/lib/api';
import type { CourseSummary } from '@/lib/types';

type SaveState = 'idle' | 'saving' | 'saved' | 'error';

function sameOrder(first: CourseSummary[], second: CourseSummary[]) {
  return (
    first.length === second.length && first.every((course, index) => String(course.id) === String(second[index]?.id))
  );
}

function reordered(courses: CourseSummary[], sourceId: string, targetId: string) {
  const sourceIndex = courses.findIndex((course) => String(course.id) === sourceId);
  const targetIndex = courses.findIndex((course) => String(course.id) === targetId);
  if (sourceIndex < 0 || targetIndex < 0 || sourceIndex === targetIndex) return courses;
  const next = [...courses];
  const [source] = next.splice(sourceIndex, 1);
  if (!source) return courses;
  next.splice(targetIndex, 0, source);
  return next;
}

function notifyCourseOrder(courses: CourseSummary[]) {
  document.dispatchEvent(
    new CustomEvent('treeEclass:courses-reordered', {
      detail: { courses: courses.map(({ id, name }) => ({ id, name })) },
    }),
  );
}

async function persistOrder(courses: CourseSummary[]) {
  await fetchJson<{ status: string }>('/api/courses/reorder', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(courses.map(({ id }) => Number(id))),
  });
}

function usePendingOrder(
  pending: CourseSummary[] | null,
  setPending: (courses: CourseSummary[] | null) => void,
  setSaveState: (state: SaveState) => void,
  savedRef: MutableRefObject<CourseSummary[]>,
  replaceOrder: (courses: CourseSummary[]) => void,
) {
  useEffect(() => {
    if (!pending) return;
    const timer = window.setTimeout(() => {
      setSaveState('saving');
      persistOrder(pending)
        .then(() => {
          savedRef.current = pending;
          setPending(null);
          setSaveState('saved');
          notifyCourseOrder(pending);
        })
        .catch(() => {
          replaceOrder(savedRef.current);
          setPending(null);
          setSaveState('error');
          notifyCourseOrder(savedRef.current);
        });
    }, 220);
    return () => window.clearTimeout(timer);
  }, [pending, replaceOrder, savedRef, setPending, setSaveState]);
}

export function useCourseOrder(courses: CourseSummary[]) {
  const [ordered, setOrdered] = useState(courses);
  const [pending, setPending] = useState<CourseSummary[] | null>(null);
  const [saveState, setSaveState] = useState<SaveState>('idle');
  const orderedRef = useRef(courses);
  const savedRef = useRef(courses);

  const replaceOrder = useCallback((next: CourseSummary[]) => {
    orderedRef.current = next;
    setOrdered(next);
  }, []);
  const previewMove = useCallback(
    (sourceId: string, targetId: string) => {
      const next = reordered(orderedRef.current, sourceId, targetId);
      if (next !== orderedRef.current) replaceOrder(next);
    },
    [replaceOrder],
  );
  const commit = useCallback(() => {
    if (!sameOrder(orderedRef.current, savedRef.current)) setPending([...orderedRef.current]);
  }, []);
  const cancel = useCallback(() => replaceOrder(savedRef.current), [replaceOrder]);

  useEffect(() => {
    if (pending || sameOrder(courses, savedRef.current)) return;
    savedRef.current = courses;
    replaceOrder(courses);
  }, [courses, replaceOrder]);
  usePendingOrder(pending, setPending, setSaveState, savedRef, replaceOrder);

  return { ordered, previewMove, commit, cancel, saveState };
}
