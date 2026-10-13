import type { CourseDetailPayload, StudyAction } from '@/lib/types';

export const OUTCOMES = [
  { value: 'completed', label: 'Completed' },
  { value: 'partial', label: 'Partly done' },
  { value: 'stuck', label: 'Stuck' },
  { value: 'deferred', label: 'Do later' },
] as const;

export type OutcomeType = (typeof OUTCOMES)[number]['value'];
export interface Reflection {
  minutes: string;
  confidence: string;
  note: string;
}

export interface OutcomeBody {
  course_id: string | number;
  event_type: OutcomeType;
  action_id?: string;
  plan_revision: string;
  actual_minutes?: number;
  confidence?: number;
  note?: string;
  idempotency_key?: string;
}

export interface RecordedEvent {
  id: string | number;
  course_id: string | number;
  action_id: string;
  plan_revision: string;
  event_type: string;
  actual_minutes: number | null;
  confidence: number | null;
  note: string | null;
}

export interface OutcomeResponse {
  status: 'recorded';
  event_id: string | number;
  event: RecordedEvent;
  overview: CourseDetailPayload | null;
  overview_error: string | null;
}

export function outcomeBody(
  action: StudyAction,
  type: OutcomeType,
  fields: Reflection,
): { body: OutcomeBody } | { error: string } {
  const body: OutcomeBody = {
    course_id: action.course_id,
    event_type: type,
    action_id: action.action_id,
    plan_revision: action.blueprint_revision_id || action.plan_revision || '',
  };
  const minutes = Number(fields.minutes);
  if (fields.minutes.trim() && (!Number.isInteger(minutes) || minutes < 1 || minutes > 1440)) {
    return { error: 'Minutes must be a whole number between 1 and 1440.' };
  }
  if (Number.isInteger(minutes) && minutes > 0) body.actual_minutes = minutes;
  if (type === 'partial' && !body.actual_minutes) {
    return { error: 'Partial progress requires a positive minutes value.' };
  }
  if (fields.confidence !== '') body.confidence = Number(fields.confidence);
  if (fields.note.length > 4000) return { error: 'Notes must be at most 4000 characters.' };
  const note = fields.note.trim();
  if (note) body.note = note;
  return { body };
}
