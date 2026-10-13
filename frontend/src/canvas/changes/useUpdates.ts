import { useEffect, useState, type Dispatch, type SetStateAction } from 'react';
import type { CourseTimelineItem } from '@/lib/types';
import { requestJson } from '../app/api';

interface UpdatesPayload {
  timeline: CourseTimelineItem[];
  has_more: boolean;
}

interface UpdatesState {
  items: CourseTimelineItem[];
  hasMore: boolean;
  busy: boolean;
  error: boolean;
}

interface PageRequest {
  offset: number;
  attempt: number;
}

async function fetchUpdates(
  courseId: string | number,
  offset: number,
  controller: AbortController,
  setState: Dispatch<SetStateAction<UpdatesState>>,
): Promise<void> {
  try {
    const path = `/api/v1/courses/${encodeURIComponent(courseId)}/updates?limit=10&offset=${offset}`;
    const payload = await requestJson<UpdatesPayload>(path, { signal: controller.signal });
    if (controller.signal.aborted) return;
    setState((previous) => ({
      items: offset ? [...previous.items, ...payload.timeline] : payload.timeline,
      hasMore: payload.has_more,
      busy: false,
      error: false,
    }));
  } catch {
    if (!controller.signal.aborted) setState((previous) => ({ ...previous, busy: false, error: true }));
  }
}

export function useCourseUpdates(courseId: string | number) {
  const [state, setState] = useState<UpdatesState>({ items: [], hasMore: false, busy: true, error: false });
  const [page, setPage] = useState<PageRequest>({ offset: 0, attempt: 0 });
  useEffect(() => {
    const controller = new AbortController();
    setState((previous) => ({ ...previous, busy: true, error: false }));
    void fetchUpdates(courseId, page.offset, controller, setState);
    return () => controller.abort();
  }, [courseId, page.offset, page.attempt]);
  const retry = () => setPage((previous) => ({ ...previous, attempt: previous.attempt + 1 }));
  const loadMore = () => {
    if (state.busy || state.error || !state.hasMore) return;
    setState((previous) => ({ ...previous, busy: true }));
    setPage({ offset: state.items.length, attempt: 0 });
  };
  return { ...state, retry, loadMore };
}
