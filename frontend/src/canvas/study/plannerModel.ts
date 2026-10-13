import type { PlannerRow, PlannerSettings, StudyPayload } from '@/lib/types';
import type { JsonValue } from '@/lib/display';

export const DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
export const COMMITMENTS = [
  { value: 'must_pass', label: 'Must pass' },
  { value: 'committed', label: 'Committed' },
  { value: 'conditional', label: 'Conditional' },
  { value: 'skipped', label: 'Skipped' },
];

export interface CourseDraft {
  row: PlannerRow;
  enabled: boolean;
  shortName: string;
  examAt: string;
  targetGrade: string;
  commitment: string;
  notes: string;
}

export interface PlannerDraft {
  courses: CourseDraft[];
  weekly: string[];
  blockMinutes: string;
  maxCourses: string;
  blackoutDates: string;
}

/** The planner endpoint consumes raw form strings; unincluded courses omit only their enabled field. */
export interface PlannerSubmission {
  block_minutes: string;
  max_courses_per_day: string;
  blackout_dates: string;
  [field: `weekly_minutes_${number}`]: string;
  [field: `short_name_${string}`]: string;
  [field: `exam_at_${string}`]: string;
  [field: `target_grade_${string}`]: string;
  [field: `commitment_${string}`]: string;
  [field: `planning_notes_${string}`]: string;
  [field: `enabled_${string}`]: 'on' | undefined;
}

export function createPlannerDraft(data: StudyPayload): PlannerDraft {
  const settings: PlannerSettings = data.planner_settings || {};
  return {
    courses: (data.planner_rows || []).map((row) => ({
      row,
      enabled: row.enabled,
      shortName: row.short_name || '',
      examAt: row.exam_at || '',
      targetGrade: String(row.target_grade ?? 5),
      commitment: row.commitment || 'conditional',
      notes: row.planning_notes || '',
    })),
    weekly: DAYS.map((_, index) => String(settings.weekly_minutes?.[String(index)] || 0)),
    blockMinutes: String(settings.block_minutes || 45),
    maxCourses: String(settings.max_courses_per_day || 3),
    blackoutDates: (settings.blackout_dates || []).join(', '),
  };
}

export function plannerPayload(draft: PlannerDraft): PlannerSubmission {
  const payload: PlannerSubmission = {
    block_minutes: draft.blockMinutes,
    max_courses_per_day: draft.maxCourses,
    blackout_dates: draft.blackoutDates,
  };
  draft.weekly.forEach((value, index) => {
    payload[`weekly_minutes_${index}`] = value;
  });
  for (const course of draft.courses) {
    const id = String(course.row.course_id);
    payload[`short_name_${id}`] = course.shortName;
    payload[`exam_at_${id}`] = course.examAt;
    payload[`target_grade_${id}`] = course.targetGrade;
    payload[`commitment_${id}`] = course.commitment;
    payload[`planning_notes_${id}`] = course.notes;
    if (course.enabled) payload[`enabled_${id}`] = 'on';
  }
  return payload;
}

function integerError(value: string, label: string, min: number, max: number): string | null {
  return /^\d+$/.test(value.trim()) && Number(value) >= min && Number(value) <= max
    ? null
    : `${label} must be a whole number between ${min} and ${max}.`;
}

function validCalendarDay(value: string): boolean {
  const date = new Date(`${value}T00:00:00Z`);
  return (
    /^\d{4}-\d{2}-\d{2}$/.test(value) && !Number.isNaN(date.getTime()) && date.toISOString().slice(0, 10) === value
  );
}

function validExamDate(value: string): boolean {
  const parts = /^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(Z|[+-]\d{2}:\d{2})?)?$/.exec(value);
  if (!parts || !validCalendarDay(parts[1]!)) return false;
  if (!parts[2]) return true;
  if (Number(parts[2]) > 23 || Number(parts[3]) > 59 || Number(parts[4] || '0') > 59) return false;
  const normalized = value.replace(' ', 'T');
  return !Number.isNaN(new Date(parts[5] ? normalized : `${normalized}Z`).getTime());
}

function courseErrors(course: CourseDraft): string[] {
  const errors: string[] = [];
  if (course.enabled && !course.examAt) errors.push('Enabled courses need an exam date.');
  if (course.examAt && !validExamDate(course.examAt)) {
    errors.push(`${course.row.course_name} has an invalid exam date.`);
  }
  const grade = Number(course.targetGrade);
  if (!course.targetGrade.trim() || !Number.isFinite(grade) || grade < 0 || grade > 10) {
    errors.push('Target grades must be between 0 and 10.');
  } else if (Math.abs(grade * 10 - Math.round(grade * 10)) > 1e-8) {
    errors.push('Target grades must use increments of 0.1.');
  }
  if (course.shortName.length > 24) errors.push('Short names must be at most 24 characters.');
  if (course.notes.length > 2000) errors.push('Planning notes must be at most 2000 characters.');
  if (!COMMITMENTS.some((option) => option.value === course.commitment)) errors.push('Choose a valid commitment.');
  return errors;
}

export function validatePlanner(draft: PlannerDraft): string[] {
  const errors: string[] = [];
  DAYS.forEach((day, index) => {
    const error = integerError(draft.weekly[index] ?? '', `${day} minutes`, 0, 1440);
    if (error) errors.push(error);
  });
  for (const error of [
    integerError(draft.blockMinutes, 'Session length', 15, 1440),
    integerError(draft.maxCourses, 'Maximum courses per day', 1, 7),
  ]) {
    if (error) errors.push(error);
  }
  draft.blackoutDates
    .trim()
    .split(/[\s,]+/)
    .filter(Boolean)
    .forEach((value) => {
      if (!validCalendarDay(value)) errors.push(`Blackout date “${value}” must use YYYY-MM-DD.`);
    });
  draft.courses.forEach((course) => errors.push(...courseErrors(course)));
  return [...new Set(errors)];
}

function hasPlannerDetail(body: JsonValue): body is { detail: JsonValue } {
  return body !== null && typeof body === 'object' && 'detail' in body;
}

function hasValidationMessage(item: JsonValue): item is { msg: string } {
  return item !== null && typeof item === 'object' && 'msg' in item && typeof item.msg === 'string';
}

/** Preserve server validation messages, including non-message details and HTTP fallback text. */
export function plannerErrorMessages(body: JsonValue, status: number): string[] {
  const detail = hasPlannerDetail(body) ? body.detail : undefined;
  if (Array.isArray(detail))
    return detail.map((item: JsonValue) => (hasValidationMessage(item) ? item.msg : String(item)));
  return [String(detail || `Could not save study plan (HTTP ${status}).`)];
}
