import type { CourseDetailPayload, RoadmapAction } from '@/lib/types';
import { intentKey } from './styles';

export type RoadmapOutcome = 'completed' | 'partial' | 'stuck' | 'deferred';

export interface RoadmapReflection {
  minutes: string;
  confidence: string;
  note: string;
}

export interface RoadmapEventBody {
  course_id: string | number;
  event_type: RoadmapOutcome;
  action_id: string;
  plan_revision: string;
  idempotency_key: string;
  actual_minutes?: number;
  confidence?: number;
  note?: string;
}

export interface RecordedRoadmapEvent {
  status: 'recorded';
  event_id?: string | number;
  event?: unknown;
  overview: CourseDetailPayload | null;
  overview_error?: string | null;
}

export function buildRoadmapEvent(
  action: RoadmapAction,
  courseId: string | number,
  revision: string | undefined,
  eventType: RoadmapOutcome,
  reflection: RoadmapReflection,
): { body: RoadmapEventBody } | { error: string } {
  const actionId = action.action_id || String(action.id || '');
  if (!revision || !actionId) return { error: 'Reopen the roadmap to load the current action revision.' };
  const minutes = Number(reflection.minutes);
  if (eventType === 'partial' && (!Number.isInteger(minutes) || minutes <= 0)) {
    return { error: 'Add minutes first.' };
  }
  if (reflection.minutes !== '' && (!Number.isInteger(minutes) || minutes < 1 || minutes > 1440)) {
    return { error: 'Minutes must be a whole number from 1 to 1440.' };
  }
  if (reflection.note.length > 4000) return { error: 'The note must be at most 4000 characters.' };
  const body: RoadmapEventBody = {
    course_id: courseId,
    action_id: actionId,
    plan_revision: revision,
    event_type: eventType,
    idempotency_key: intentKey('roadmap'),
  };
  if (Number.isInteger(minutes) && minutes > 0) body.actual_minutes = minutes;
  if (reflection.confidence !== '') body.confidence = Number(reflection.confidence);
  if (reflection.note.trim()) body.note = reflection.note.trim();
  return { body };
}
