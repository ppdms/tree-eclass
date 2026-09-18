/**
 * Thin fetch helpers for the workspace.
 *
 * Every write the workspace makes is idempotent on the server, so the client's
 * job is only to supply a stable key and to survive a failed round trip without
 * inventing state. Nothing here retries silently: a lost write surfaces, because
 * the alternative is a reading ledger that quietly disagrees with reality.
 */

import { isServer } from '@/lib/display';
import type {
  Annotation,
  AnnotationRect,
  PagesPayload,
  PracticeView,
  SessionContext,
  SessionInfo,
} from '@/features/session/reader/types';

/** A stable key for one client-side event, safe for the server's key rules. */
export function requestKey(prefix: string): string {
  const random =
    !isServer() && crypto.randomUUID
      ? crypto.randomUUID().replace(/-/g, '')
      : Math.random().toString(36).slice(2) + Date.now().toString(36);
  return `${prefix}-${random}`.slice(0, 128);
}

async function request<T>(url: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!response.ok) {
    let detail = `Request failed (${response.status})`;
    try {
      const body = await response.json();
      if (body?.detail) detail = body.detail;
    } catch {
      /* A non-JSON error body is not more informative than the status. */
    }
    // SAFETY: the status is attached to the error object for callers that
    // need it (e.g. the route loader maps loadData failures to 404 vs 500).
    const error = new Error(detail) as Error & { status?: number };
    error.status = response.status;
    throw error;
  }
  // SAFETY: a 204 has no body; the null cast is the documented contract for
  // delete endpoints. Other statuses are the caller's T contract.
  return response.status === 204 ? (null as T) : (response.json() as Promise<T>);
}

const asQuery = (params: Record<string, string | number | boolean | null | undefined>): string =>
  new URLSearchParams(
    Object.entries(params)
      .filter(([, value]) => value !== undefined && value !== null)
      .map(([key, value]) => [key, String(value)]),
  ).toString();

export interface SessionStartPayload {
  course_id: string | number;
  session_key: string;
  action_id?: string;
  unit_key?: string;
  plan_revision?: string;
  planned_minutes?: number | null;
}

export interface SessionHeartbeatPayload {
  session_id: string | number;
  sequence: number;
  document_id: string | number;
  page_number: number;
  active: boolean;
  interval_seconds: number;
}

export interface SessionFinishPayload {
  session_id: string | number;
  outcome: string;
  note: string | null;
}

export interface AnnotationCreatePayload {
  course_id: string | number;
  document_id: string | number;
  page_number: number;
  kind: string;
  quote?: string;
  prefix?: string;
  suffix?: string;
  char_start?: number;
  char_end?: number;
  rects?: AnnotationRect[];
  color?: string;
  body?: string | null;
  action_id?: string;
  unit_key?: string;
  plan_revision?: string;
  session_id?: string | number;
  idempotency_key: string;
}

export interface AnnotationUpdatePayload {
  status?: string;
  body?: string | null;
  color?: string;
}

export interface PracticeAttemptPayload {
  course_id: string | number;
  question_id: string | number;
  outcome: string;
  confidence: number | null;
  seconds: number;
  answer: string | null;
  idempotency_key: string;
}

export interface ActionEventPayload {
  course_id: string | number;
  event_type: string;
  action_id?: string;
  plan_revision?: string;
  actual_minutes?: number;
  confidence?: number;
  note?: string;
  idempotency_key?: string;
}

export const api = {
  context: (courseId: string | number, actionId: string | null, documentId: string | null, signal?: AbortSignal) =>
    request<SessionContext>(
      `/api/study/session/context?${asQuery({
        course_id: courseId,
        include_practice: false,
        action_id: actionId ?? undefined,
        document_id: documentId ?? undefined,
      })}`,
      { signal },
    ),

  pages: (courseId: string | number, documentId: string | number, first: number, last: number, signal?: AbortSignal) =>
    request<PagesPayload>(
      `/api/study/document/${documentId}/pages?${asQuery({
        course_id: courseId,
        first,
        last,
      })}`,
      { signal },
    ),

  startSession: (payload: SessionStartPayload) =>
    request<{ session: SessionInfo }>('/api/study/session/start', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  heartbeat: (payload: SessionHeartbeatPayload) =>
    request<{ session: SessionInfo }>('/api/study/session/heartbeat', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  finishSession: (payload: SessionFinishPayload) =>
    request<SessionInfo>('/api/study/session/finish', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  annotations: (courseId: string | number, documentId: string | number, signal?: AbortSignal) =>
    request<{ annotations: Annotation[] }>(
      `/api/study/annotations?${asQuery({
        course_id: courseId,
        document_id: documentId,
      })}`,
      { signal },
    ),

  createAnnotation: (payload: AnnotationCreatePayload) =>
    request<{ annotation: Annotation }>('/api/study/annotations', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  updateAnnotation: (id: string | number, payload: AnnotationUpdatePayload) =>
    request<{ annotation: Annotation }>(`/api/study/annotations/${id}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    }),

  deleteAnnotation: (id: string | number) => request<null>(`/api/study/annotations/${id}`, { method: 'DELETE' }),

  practiceAttempt: (payload: PracticeAttemptPayload) =>
    request<{ practice: PracticeView }>('/api/study/practice/attempt', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),

  actionEvent: (payload: ActionEventPayload) =>
    request<unknown>('/api/v1/study/actions/event', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
};
