// ──────────────────────────────────────────────────────────────────────────
// Shared API data contracts.
//
// These mirror the JSON payloads the native Go backend serves. They are the
// single source of truth for the frontend's data shapes, so every page and
// workspace module imports from here rather than re-declaring its own (which
// is how `as any` leaks in). Fields that the backend may omit are optional;
// fields that arrive as either a string or a number are typed as such.
//
// Blueprint / enrichment domain types live in ./blueprint and are re-exported
// here so `@/lib/types` stays the single import surface.
// ──────────────────────────────────────────────────────────────────────────

import type { BlueprintCourse, FileInsightAi, RoadmapBlueprint } from './blueprint';

export type {
  BlueprintCourse,
  CoverageGap,
  FileInsightAi,
  QuestionFamily,
  ReadinessReason,
  RoadmapAction,
  RoadmapBlueprint,
  RoadmapConflict,
  RoadmapUnit,
} from './blueprint';

/** A file-level change inside a course change. */
export interface ChangeItem {
  id?: string | number;
  change_type?: string;
  type?: string;
  file_path?: string;
  path?: string;
  name?: string;
  display_name?: string;
  redirect_url?: string | null;
  diff_webdav_path?: string | null;
}

/** One item inside an activity group. */
export interface ActivityItem {
  id?: string | number;
  timestamp?: string;
  type?: string;
  link?: string;
  course_id?: string | number;
  change_no?: string;
  course_name?: string;
  course_short_name?: string;
  description?: string;
  changes?: ChangeItem[];
}

export type ActivityGroupType = 'change' | 'announcement' | 'assignment';

/** A grouped activity/announcement entry. */
export interface ActivityGroup {
  id: string | number;
  title: string;
  type: ActivityGroupType;
  importance?: string;
  items?: ActivityItem[];
  link?: string;
  timestamp?: string;
}

/** The `/api/v1/inbox` payload. */
export interface InboxPayload {
  groups: ActivityGroup[];
  next_offset: number;
  has_more: boolean;
}

export type ActivityFilter = 'all' | 'unread' | 'important' | 'announcements' | 'changes';

export interface ActivityDay {
  label: string;
  groups: ActivityGroup[];
}

// ── Courses ────────────────────────────────────────────────────────────────

export type StudyLevelValue = 0 | 1 | 2 | 3 | 4 | 5;

export interface StudyDistribution {
  [level: number]: number;
}

export interface CourseSummary {
  id: string | number;
  name: string;
  code?: string;
  hidden?: boolean;
  completion_ratio?: number;
  study_distribution?: StudyDistribution;
  total_files?: number;
  indexed_files?: number;
  webdav_folder?: string;
  short_name?: string;
  stale?: boolean;
}

export interface CourseMaterial {
  document_id?: string | number;
  id?: string | number;
  source_path: string;
  course_id: string | number;
  course_name?: string;
  display_name?: string;
  indexed_at?: string;
}

export interface CoursesCoverage {
  courses: CourseSummary[];
  recent_materials: CourseMaterial[];
  study_levels?: Record<string, Record<string, StudyLevelValue>>;
  generated_at?: string | null;
  generation?: number;
  stale?: boolean;
  status?: 'ready' | 'building';
}

export interface NavigationCourses {
  courses: Array<Pick<CourseSummary, 'id' | 'name'>>;
}

// ── Course detail ──────────────────────────────────────────────────────────

export interface FileInsight {
  guide_available?: boolean;
  source_hash?: string;
  enrichment_generated_at?: string;
  enrichment_analysis_version?: string;
  id?: string;
  course_id?: string | number;
  document_kind?: string;
  source_status?: string;
  source_diagnostic_reason?: string;
  source_error?: string;
  page_count?: number;
  unit_name?: string;
  word_count?: number;
  reading_minutes?: number;
  source_size_bytes?: number;
  complexity_label?: string;
  complexity_score?: number;
  enrichment_status?: string;
  ai_processing_enabled?: boolean;
  page_analysis_ready?: number;
  page_analysis_total?: number;
  page_analysis_enabled?: boolean;
  enrichment_model?: string;
  enrichment_error?: string;
  warning_count?: number;
  summary?: string;
  ai?: FileInsightAi;
  warnings?: unknown[];
}

export interface CourseFileNode {
  name: string;
  url?: string;
  local_path?: string;
  last_updated?: string;
  redirect_url?: string;
  file_size?: number;
  files?: CourseFileNode[];
  children?: CourseFileNode[];
}

export type ExternalMaterialType =
  | 'past_paper'
  | 'student_notes'
  | 'study_guide'
  | 'textbook'
  | 'exercise_solution'
  | 'lecture_material'
  | 'other';

export interface ExternalMaterial {
  id: string;
  document_id: string;
  course_id: string | number;
  display_name: string;
  source_path: string;
  source_origin: 'external';
  material_type: ExternalMaterialType;
  classification_source: 'ai' | 'folder' | 'inferred' | 'manual' | 'pending';
  source_label?: string;
  document_kind?: string;
  status: string;
  diagnostic_reason?: string;
  source_modified_at?: string;
  indexed_at?: string;
  source_size_bytes?: number;
  page_count?: number;
  reading_minutes?: number;
  renderable?: boolean;
  open_url?: string;
  download_url?: string;
}

export interface EvidenceLink {
  evidence_class?: string;
  evidence_ref?: string;
  label?: string;
  title?: string;
  source_name?: string;
  url?: string;
  source_path?: string;
  document_id?: string | number;
  renderable?: boolean;
  page_number?: number;
}

export interface CourseTimelineItem {
  type?: string;
  id?: string | number;
  sort_key?: string;
  title?: string;
  message?: string;
  description?: string;
  link?: string;
  timestamp?: string;
  course_id?: string | number;
  course_name?: string;
  course_short_name?: string;
  change_no?: string;
  changes?: ChangeItem[];
}

export interface CourseVersion {
  display_name?: string;
  file_path: string;
  timestamp?: string;
  diff_webdav_path?: string;
  redirect_url?: string;
  version_webdav_path?: string;
}

export interface CourseDetailPayload {
  course: CourseSummary & { id: string | number };
  tree?: CourseFileNode;
  file_insights?: Record<string, FileInsight>;
  external_materials?: ExternalMaterial[];
  study_levels?: Record<string, StudyLevelValue>;
  files_with_versions?: string[];
  collapsed_folders?: string[];
  expanded_folders?: string[];
  folders_with_deleted?: string[];
  study_distribution?: StudyDistribution;
  course_blueprint?: RoadmapBlueprint;
  timeline?: CourseTimelineItem[];
}

export interface CourseFilesPayload {
  tree?: CourseFileNode;
  file_insights?: Record<string, FileInsight>;
  external_materials?: ExternalMaterial[];
  study_levels?: Record<string, StudyLevelValue>;
  files_with_versions?: string[];
  collapsed_folders?: string[];
  expanded_folders?: string[];
  folders_with_deleted?: string[];
}

// ── Change detail ──────────────────────────────────────────────────────────

export interface ChangeDetailPayload {
  course?: { name?: string };
  change_record?: { message?: string; timestamp?: string };
  changes?: ChangeItem[];
  webdav_folder?: string;
}

// ── Study ──────────────────────────────────────────────────────────────────

export interface StudyAction {
  action_id?: string;
  action_type?: string;
  kind?: string;
  course_id: string | number;
  course_name?: string;
  short_name?: string;
  instruction?: string;
  recommended_action?: string;
  file_name?: string;
  source_path?: string;
  file_path?: string;
  minutes?: number;
  reading_minutes?: number;
  estimated_minutes?: number;
  remaining_minutes?: number;
  scheduled_date?: string;
  days_to_exam?: number;
  redirect_url?: string;
  level?: StudyLevelValue;
  unit_title?: string;
  display_name?: string;
  rationale?: string;
  success_criterion?: string;
  blueprint_revision_id?: string;
  plan_revision?: string;
  evidence_links?: EvidenceLink[];
  priority?: number;
}

export interface StudyInboxItem {
  course_id: string | number;
  course_name?: string;
  file_name?: string;
  file_path?: string;
  study_level?: StudyLevelValue;
  priority?: number;
}

export interface PlannerRow {
  course_id: string | number;
  course_name: string;
  enabled: boolean;
  short_name?: string;
  exam_at?: string;
  commitment?: string;
  target_grade?: number;
  planning_notes?: string;
}

export interface PlannerSettings {
  weekly_minutes?: Record<string, number>;
  block_minutes?: number;
  max_courses_per_day?: number;
  blackout_dates?: string[];
}

export interface StudyPayload {
  study_projection_status?: string;
  adaptive_plan?: {
    next_session?: StudyAction;
    today_queue?: StudyAction[];
    blueprint_courses?: BlueprintCourse[];
    pending_blueprints?: BlueprintCourse[];
    missing_blueprints?: BlueprintCourse[];
  };
  study_intelligence?: {
    focus_queue?: StudyAction[];
    coverage?: { enriched?: number; total?: number };
  };
  inbox?: StudyInboxItem[];
  adaptive_plan_available?: boolean;
  study_intelligence_available?: boolean;
  knowledge_meta?: { freshness?: string };
  planner_rows?: PlannerRow[];
  planner_settings?: PlannerSettings;
  selected_course?: CourseSummary | null;
  generated_at?: string | null;
  generation?: number;
  stale?: boolean;
  status?: 'ready' | 'building';
}

// ── Exercises ──────────────────────────────────────────────────────────────

export interface Exercise {
  fetched_at?: string;
  course_id: string | number;
  exercise_id: string | number;
  title?: string;
  link?: string;
  submission_status?: string;
  deadline?: string;
  deadline_short?: string;
  _time_label?: string;
  _urgency?: string;
  ignored?: boolean;
  grade?: string;
  course_name?: string;
  description?: string;
  work_type?: string;
  assignment_file_url?: string;
  assignment_file_name?: string;
  grade_comments?: string;
  submission_date?: string;
}

export interface ExercisesPayload {
  exercises: Exercise[];
}
