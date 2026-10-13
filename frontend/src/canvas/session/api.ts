import { requestJson } from '../app/api';
import type {
  AttemptPayload,
  AttemptResult,
  Beat,
  Context,
  FinishResult,
  Id,
  Mark,
  MarkCreate,
  Outcome,
  PageInsight,
  Practice,
  SessionQuery,
  Sitting,
  StartPayload,
} from './types';

export function requestKey(prefix: string): string {
  return `${prefix}_${crypto.randomUUID().replaceAll('-', '')}`;
}
export function message(cause: unknown): string {
  const raw = cause instanceof Error ? cause.message : String(cause);
  if (/network|failed to fetch|connection/i.test(raw)) return 'Could not reach the server. Try again.';
  return (
    raw
      .replace(/https?:\/\/[^\s'"]+/g, '')
      .replace(/['"`][^'"`]*['"`]/g, '')
      .trim() || 'The request could not be completed. Try again.'
  );
}
function hasErrorStatus(cause: unknown): cause is { status: unknown } {
  return cause !== null && typeof cause === 'object' && 'status' in cause;
}
export function errorStatus(cause: unknown): number | undefined {
  return hasErrorStatus(cause) ? Number(cause.status) : undefined;
}
/** Match native rune bounds without allocating a second copy of a learner draft. */
export function codePointLength(value: string): number {
  let count = 0;
  for (let index = 0; index < value.length; index += 1) {
    const high = value.charCodeAt(index);
    const low = value.charCodeAt(index + 1);
    if (high >= 0xd800 && high <= 0xdbff && low >= 0xdc00 && low <= 0xdfff) index += 1;
    count += 1;
  }
  return count;
}
function write<T, Payload = never>(path: string, method: string, payload?: Payload, signal?: AbortSignal): Promise<T> {
  return requestJson<T>(path, {
    method,
    signal,
    headers: { 'Content-Type': 'application/json' },
    body: payload === undefined ? undefined : JSON.stringify(payload),
  });
}
export const api = {
  context(query: SessionQuery, signal: AbortSignal): Promise<Context> {
    const params = new URLSearchParams({ course_id: query.courseId, include_practice: 'false' });
    if (query.actionId !== null) params.set('action_id', query.actionId);
    if (query.documentId !== null) params.set('document_id', query.documentId);
    return requestJson(`/api/study/session/context?${params}`, { signal });
  },
  start(payload: StartPayload, signal: AbortSignal): Promise<{ session: Sitting; next_sequence: number }> {
    return write('/api/study/session/start', 'POST', payload, signal);
  },
  heartbeat(payload: Beat): Promise<{ status: string; session: Sitting }> {
    return requestJson('/api/study/session/heartbeat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      keepalive: true,
    });
  },
  finish(
    payload: { session_id: Id; outcome: Outcome; note: string | null; confidence: number | null },
    signal: AbortSignal,
  ): Promise<FinishResult> {
    return write('/api/study/session/finish', 'POST', payload, signal);
  },
  annotations(course: string, document: string, signal: AbortSignal): Promise<{ annotations: Mark[] }> {
    const params = new URLSearchParams({ course_id: course, document_id: document });
    return requestJson(`/api/study/annotations?${params}`, { signal });
  },
  create(payload: MarkCreate, signal: AbortSignal): Promise<{ annotation: Mark }> {
    return write('/api/study/annotations', 'POST', payload, signal);
  },
  update(id: Id, patch: { body?: string; color?: string }, signal: AbortSignal): Promise<{ annotation: Mark }> {
    return write(`/api/study/annotations/${encodeURIComponent(String(id))}`, 'PATCH', patch, signal);
  },
  delete(id: Id, signal: AbortSignal): Promise<{ status: 'deleted'; id: Id }> {
    return write(`/api/study/annotations/${encodeURIComponent(String(id))}`, 'DELETE', undefined, signal);
  },
  pages(
    course: string,
    document: string,
    page: number,
    signal: AbortSignal,
  ): Promise<{ source_hash: string; pages: PageInsight[] }> {
    const params = new URLSearchParams({ course_id: course, first: String(page), last: String(page) });
    return requestJson(`/api/study/document/${encodeURIComponent(document)}/pages?${params}`, { signal });
  },
  practice(course: string, unit: string, signal: AbortSignal): Promise<Practice> {
    const params = new URLSearchParams({ course_id: course });
    if (unit) params.set('unit_key', unit);
    return requestJson(`/api/study/practice?${params}`, { signal });
  },
  attempt(payload: AttemptPayload, signal: AbortSignal): Promise<AttemptResult> {
    return write('/api/study/practice/attempt', 'POST', payload, signal);
  },
};
