// ──────────────────────────────────────────────────────────────────────────
// Blueprint / enrichment domain types.
//
// Named contracts for the AI-synthesized payloads (course roadmap blueprint,
// file insight enrichment, adaptive plan descriptors). Kept separate from
// types.ts so that file stays under the 400-line gate; types.ts re-exports
// these so `@/lib/types` remains the single import surface.
// ──────────────────────────────────────────────────────────────────────────

import type { EvidenceLink, StudyAction } from './types';

/** The AI-generated enrichment payload attached to a file insight. */
export interface FileInsightAi {
  summary?: string;
  importance?: string;
  importance_reason?: string;
  difficulty?: string;
  difficulty_reason?: string;
  assessment_relevance?: string;
  assessment_reason?: string;
  course_role?: string;
  material_type?: string;
  recommended_action?: string;
  topics?: string[];
  prerequisites?: string[];
  learning_objectives?: string[];
  visual_content?: string[];
  notable_items?: string[];
  overlap?: string;
  related_materials?: { path?: string; name?: string }[];
  vision_pages?: number[];
}

/** A question-family entry in the blueprint synthesis. */
export interface QuestionFamily {
  key?: string;
  name?: string;
  response_mode?: string;
  priority?: string;
  confidence?: string;
  observed_count?: number;
  observed_out_of?: number;
  estimated_marks_percent?: number;
  unit_keys?: string[];
  evidence_refs?: string[];
  evidence_links?: EvidenceLink[];
}

/** A source-conflict entry in the blueprint synthesis. */
export interface RoadmapConflict {
  summary?: string;
  status?: string;
  resolution?: string;
  evidence_refs?: string[];
  evidence_links?: EvidenceLink[];
}

/** A coverage-gap entry in the blueprint synthesis. */
export interface CoverageGap {
  summary?: string;
  severity?: string;
  recommended_action?: string;
  evidence_refs?: string[];
  evidence_links?: EvidenceLink[];
}

/** A human-readable readiness note; either a plain string or a structured reason. */
export interface ReadinessReason {
  reason?: string;
  summary?: string;
  message?: string;
}

export interface SourceReadiness extends ReadinessReason {
  settled?: boolean;
  ready?: boolean;
  degraded?: boolean;
  blocking_reasons?: string[];
  ready_documents?: number;
  ready_document_insights?: number;
  documents_without_enrichment?: number;
  active_extraction_jobs?: number;
  active_document_enrichments?: number;
  active_page_enrichments?: number;
  page_enrichments?: Record<string, number>;
  failed_documents?: number;
  failed_extraction_jobs?: number;
  failed_document_enrichments?: number;
  failed_page_enrichments?: number;
}

/** A course with a usable (or pending/missing) blueprint in the adaptive plan. */
export interface BlueprintCourse {
  course_id?: string | number;
  course_name?: string;
  short_name?: string;
  revision_id?: string;
  revision_number?: string | number;
  status?: string;
  readiness?: string;
  readiness_reason?: string;
  usable?: boolean;
  generated_at?: string;
  source_freshness?: string;
  completed_actions?: number;
  total_actions?: number;
  percent?: number;
  next_action?: StudyAction;
}

export interface RoadmapBlueprint {
  actions_deferred?: boolean;
  total_units?: number;
  actions?: RoadmapAction[];
  blueprint?: {
    deferred_sections?: { strategy: boolean; support: boolean };
    exam_strategy?: {
      confidence?: string | number;
      objective?: string;
      approach?: string;
      target_score_percent?: number;
      evidence_links?: EvidenceLink[];
    };
    question_families?: QuestionFamily[];
    conflicts?: RoadmapConflict[];
    coverage_gaps?: CoverageGap[];
    units?: RoadmapUnit[];
  };
  readiness?: {
    state?: string;
    reason?: string | ReadinessReason;
    source_readiness?: SourceReadiness;
    refresh_pending?: boolean;
  };
  progress?: {
    percent?: number;
    completed_actions?: number;
    total_actions?: number;
    next_action?: RoadmapAction;
    next_action_id?: string;
  };
  coverage?: {
    fully_covered?: boolean;
    plan_snapshot?: {
      documents_with_ready_insight?: number;
      eligible_documents?: number;
    };
  };
  usable?: boolean;
  revision_id?: string;
  revision_hash?: string;
  generated_at?: string;
  model?: string;
  practice?: {
    enabled?: boolean;
    pending_units?: number;
  };
}

export interface RoadmapAction {
  unit_title?: string;
  action_id?: string;
  id?: string | number;
  kind?: string;
  action_type?: string;
  instruction?: string;
  success_criterion?: string;
  status?: string;
  estimated_minutes?: number;
  remaining_minutes?: number;
  progress_minutes?: number;
  evidence_links?: EvidenceLink[];
  unit_key?: string;
}

export interface RoadmapUnit {
  action_ids?: string[];
  key?: string;
  title?: string;
  priority?: string;
  completed_actions?: number;
  total_actions?: number;
  estimated_minutes?: number;
  objective?: string;
  depends_on?: string[];
  evidence_links?: EvidenceLink[];
  actions?: RoadmapAction[];
}
