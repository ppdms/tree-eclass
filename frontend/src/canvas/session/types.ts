import type { StudyAction } from '@/lib/types';
import type { Annotation, AnnotationRect, ReaderSelection, SessionDocument } from './reader/types';

export type Id = string | number;
export type Outcome = 'completed' | 'partial' | 'stuck' | 'deferred' | 'abandoned';
export type RecallOutcome = 'correct' | 'partial' | 'incorrect' | 'skipped';
export type DrawerTab = 'insight' | 'marks' | 'recall';
export type Source = SessionDocument & { source_hash?: string; mime_type?: string };
export type Mark = Annotation & { document_id?: string; source_hash?: string };
export interface Sitting {
  id: Id;
  course_id: Id;
  action_id: string;
  unit_key: string;
  plan_revision: string;
  client_session_key: string;
  planned_minutes: number | null;
  active_seconds: number;
  visible_seconds: number;
  ended_at: string | null;
  started_at: string | null;
  last_seen_at: string | null;
  outcome: Outcome | null;
  note: string | null;
  confidence: number | null;
  study_event_id: number | null;
}
/** The session-start mutation, including the durable client key and authoritative plan context. */
export interface StartPayload {
  course_id: Id;
  session_key: string;
  action_id: string;
  unit_key: string;
  plan_revision: string;
  planned_minutes: number | null;
}
export interface Reading {
  active_seconds: number;
  visible_seconds: number;
  active_minutes: number;
  pages_read: number;
  documents_read: number;
  pages: { document_id: string; page_number: number; active_seconds: number; visible_seconds: number }[];
}
export interface FinishResult {
  status: 'closed';
  session: Sitting;
  measured_minutes: number;
  study_event_id: number | null;
  reading: Reading;
}
export interface Context {
  course: { course_id?: Id; short_name?: string; name?: string };
  action: StudyAction | null;
  unit_key: string;
  plan_revision: string;
  documents: Source[];
  annotations: Mark[];
  annotations_document_id?: string;
  notice?: string;
}
export interface SessionQuery {
  courseId: string;
  actionId: string | null;
  documentId: string | null;
  page: number;
  identity: string;
}
export interface MutationContext {
  course_id: Id;
  document_id: string;
  source_hash: string;
  action_id: string;
  unit_key: string;
  plan_revision: string;
  session_id?: Id;
}
export interface Selection extends ReaderSelection {
  context: MutationContext;
}
export interface MarkCreate extends MutationContext {
  page_number: number;
  kind: 'highlight' | 'note' | 'question' | 'bookmark';
  color: string;
  body?: string | null;
  quote?: string;
  prefix?: string;
  suffix?: string;
  char_start?: number;
  char_end?: number;
  rects?: AnnotationRect[];
  idempotency_key: string;
}
export interface Beat {
  session_id: Id;
  sequence: number;
  document_id: string;
  page_number: number;
  interval_seconds: number;
  active: boolean;
  source_hash: string;
}
export interface PracticeQuestion {
  question_id?: Id;
  id?: Id;
  prompt?: string;
  expected_answer?: string;
  answer_points?: string[];
  queue_rank?: number;
  unit_title?: string;
  unit_key?: string;
}
export interface Practice {
  enabled?: boolean;
  units?: { unit_key?: string; title?: string; questions?: PracticeQuestion[] }[];
  totals?: { questions?: number; due?: number; learned?: number };
}
export interface AttemptPayload {
  course_id: Id;
  question_id: Id;
  outcome: RecallOutcome;
  answer: string | null;
  confidence: null;
  seconds: number;
  idempotency_key: string;
}
export interface AttemptResult {
  status: 'recorded';
  attempt: { id: Id; question_id: Id; outcome: RecallOutcome };
  practice: Practice | null;
  practice_error: string | null;
}
export interface PageInsight {
  page_number: number;
  status: string;
  stale?: boolean;
  model?: string;
  insight?: Record<string, string | string[] | undefined>;
}
