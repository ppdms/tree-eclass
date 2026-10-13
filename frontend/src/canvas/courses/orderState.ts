import type { CoursesCoverage, CourseSummary } from '@/lib/types';

export interface CoursesCatalogData extends CoursesCoverage {
  /** Local request-start ordering only; never sent to or read from the API. */
  requestSequence: number;
}

export interface OrderSession {
  ordered: CourseSummary[];
  saved: CourseSummary[];
  latest: CoursesCatalogData;
  pending: CourseSummary[] | null;
  dragging: boolean;
  savedSequence: number;
}

let sequence = 0;

/** Requests and completed writes share one worker-local monotonic ordering. */
export function nextCourseSequence() {
  sequence += 1;
  return sequence;
}

export function createOrderSession(data: CoursesCatalogData): OrderSession {
  return {
    ordered: data.courses,
    saved: data.courses,
    latest: data,
    pending: null,
    dragging: false,
    savedSequence: 0,
  };
}

/** Incoming membership and objects are authoritative; keep the requested relative ID order. */
export function mergeCourseMetadata(order: CourseSummary[], incoming: CourseSummary[]) {
  const indexed = new Map(incoming.map((course) => [String(course.id), course]));
  const next: CourseSummary[] = [];
  for (const course of order) {
    const current = indexed.get(String(course.id));
    if (!current) continue;
    next.push(current);
    indexed.delete(String(course.id));
  }
  next.push(...indexed.values());
  return next;
}

export function refreshOrderSession(session: OrderSession, incoming: CoursesCatalogData) {
  if (incoming.requestSequence <= session.latest.requestSequence) return;
  session.latest = incoming;
  const authoritativeOrder = incoming.requestSequence > session.savedSequence;
  session.saved = authoritativeOrder ? incoming.courses : mergeCourseMetadata(session.saved, incoming.courses);
  const localEdit = session.dragging || session.pending !== null;
  session.ordered =
    localEdit || !authoritativeOrder ? mergeCourseMetadata(session.ordered, incoming.courses) : session.saved;
  if (session.pending) session.pending = mergeCourseMetadata(session.pending, incoming.courses);
}

export function confirmSavedOrder(session: OrderSession, attempted: CourseSummary[]) {
  session.saved = mergeCourseMetadata(attempted, session.latest.courses);
  session.savedSequence = nextCourseSequence();
  session.pending = null;
  session.ordered = session.saved;
}
