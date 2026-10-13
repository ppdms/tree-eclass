import type { CourseDetailPayload, RoadmapAction, RoadmapUnit } from '@/lib/types';

export const COURSE_TABS = [
  { id: 'overview', label: 'Overview' },
  { id: 'roadmap', label: 'Roadmap' },
  { id: 'files', label: 'Files' },
  { id: 'activity', label: 'Updates' },
  { id: 'settings', label: 'Settings' },
] as const;

export type CourseTab = (typeof COURSE_TABS)[number]['id'];

export function isCourseTab(value: string): value is CourseTab {
  return COURSE_TABS.some((tab) => tab.id === value);
}

export function tabFromHash(hash: string): CourseTab {
  const value = hash.startsWith('#') ? hash.slice(1) : hash;
  return isCourseTab(value) ? value : 'overview';
}

export function courseStudyHref(courseId: string | number): string {
  return `/study?course_id=${encodeURIComponent(String(courseId))}`;
}

export function shortInstruction(value?: string): string {
  const instruction = String(value || 'Choose your next study action');
  const lead = instruction.split(/\s+covering\s+/i)[0] || instruction;
  return lead.replace(/[.:]$/, '');
}

export function actionUnitTitle(units: RoadmapUnit[], action?: RoadmapAction): string | undefined {
  if (!action) return undefined;
  const actionId = action.action_id || action.id;
  const unit = units.find((item) =>
    item.actions?.some((candidate) => (candidate.action_id || candidate.id) === actionId),
  );
  return action.unit_title || unit?.title;
}

export function headerProgress(data: CourseDetailPayload) {
  const progress = data.course_blueprint?.progress || {};
  const completed = Number(progress.completed_actions || 0);
  const total = Number(progress.total_actions || 0);
  return { completed, total, percent: total ? Math.round((completed / total) * 100) : 0 };
}

export function overviewPercent(data: CourseDetailPayload): number {
  return Math.round(Number(data.course_blueprint?.progress?.percent || 0));
}
