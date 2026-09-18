import type { StudyAction } from '@/lib/types';

// ── Study session / workspace ──────────────────────────────────────────────

export const HIGHLIGHT_COLORS = {
  yellow: 'rgb(250 204 21 / .35)',
  green: 'rgb(163 230 53 / .35)',
  blue: 'rgb(147 197 253 / .35)',
  pink: 'rgb(251 113 133 / .35)',
} satisfies Record<string, string>;

export type HighlightColor = keyof typeof HIGHLIGHT_COLORS;

export interface SessionDocument {
  document_id: string | number;
  display_name?: string;
  file_name?: string;
  page_count?: number;
  renderable?: boolean;
  document_kind?: string;
  download_url?: string;
  content_url?: string;
}

export interface AnnotationRect {
  x: number;
  y: number;
  w: number;
  h: number;
}

export type AnnotationKind = 'highlight' | 'note' | 'question' | 'bookmark';
export type AnnotationStatus = 'active' | 'deleted' | 'orphaned';

export interface Annotation {
  id: string | number;
  kind: AnnotationKind;
  status: AnnotationStatus;
  page_number: number;
  quote?: string;
  prefix?: string;
  suffix?: string;
  char_start?: number;
  char_end?: number;
  rects?: AnnotationRect[];
  color?: string;
  body?: string | null;
}

export interface SessionInfo {
  id: string | number;
  active_seconds?: number;
}

export interface PracticeQuestion {
  question_id?: string | number;
  id?: string | number;
  prompt?: string;
  expected_answer?: string;
  answer_points?: string[];
  common_mistakes?: string[];
  state?: string;
  difficulty?: string;
  estimated_minutes?: number;
  attempts?: number;
  queue_rank?: number;
  unit_title?: string;
  unit_key?: string;
  response_mode?: string;
}

export interface PracticeUnit {
  title?: string;
  questions?: PracticeQuestion[];
}

export interface PracticeView {
  enabled?: boolean;
  units?: PracticeUnit[];
  totals?: { due?: number; learned?: number; questions?: number };
  pending_units?: number;
}

export interface SessionContext {
  annotations_document_id?: string;
  course?: { short_name?: string; name?: string };
  action?: StudyAction;
  documents?: SessionDocument[];
  unit_key?: string;
  plan_revision?: string;
  annotations?: Annotation[];
  practice?: PracticeView;
}

export interface PageInsight {
  page_number: number;
  status: string;
  stale?: boolean;
  model?: string;
  insight?: {
    summary?: string;
    key_points?: string[];
    definitions?: string[];
    formulas?: string[];
    examples?: string[];
    assessment_clues?: string[];
    visuals?: string[];
    references?: string[];
  };
}

export interface PagesPayload {
  pages?: PageInsight[];
}
