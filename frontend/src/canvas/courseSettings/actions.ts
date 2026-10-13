import type { CourseSummary } from '@/lib/types';
import { requestJson } from '../app/api';

export type CourseAction = 'check' | 'reset' | 'hide' | 'delete';

export interface CourseConfirmation {
  action: CourseAction;
  title: string;
  label: string;
  message: string;
  target: string;
}

export function courseConfirmation(course: CourseSummary, action: CourseAction): CourseConfirmation {
  const target = `/courses/${encodeURIComponent(String(course.id))}`;
  if (action === 'delete')
    return {
      action,
      title: 'Delete course?',
      label: 'Delete course',
      target: '/courses',
      message:
        `Delete ${course.name} permanently? This removes its synchronized data, ` +
        'including your notes and highlights, and cannot be undone.',
    };
  if (action === 'hide')
    return {
      action,
      title: 'Hide course?',
      label: 'Hide course',
      target: '/courses',
      message: `Hide ${course.name}? Its files stay synchronized but leave the daily workspace.`,
    };
  if (action === 'reset')
    return {
      action,
      title: 'Reset course data?',
      label: 'Reset data',
      target,
      message: `Reset the file tree and change history for ${course.name}? Course settings will stay.`,
    };
  return {
    action,
    title: 'Run course check?',
    label: 'Run check',
    target,
    message: `Run a fresh check for ${course.name}?`,
  };
}

export interface CourseMutationResult {
  status: string;
  id?: string | number;
  hidden?: boolean;
  message?: string;
}

export function renameCourse(courseId: string | number, name: string): Promise<CourseMutationResult> {
  return requestJson<CourseMutationResult>(`/api/v1/courses/${encodeURIComponent(String(courseId))}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: name.trim() }),
  });
}

export function runCourseAction(action: CourseAction, courseId: string | number): Promise<CourseMutationResult> {
  const headers = new Headers({ 'Content-Type': 'application/json', Accept: 'application/json' });
  if (action === 'reset' || action === 'delete') {
    // Destructive replays fail, so each newly confirmed intent needs a fresh key.
    headers.set('X-Tree-Eclass-Confirmation', `${action}:${courseId}`);
    headers.set('X-Idempotency-Key', globalThis.crypto?.randomUUID?.() || `${action}:${courseId}:${Date.now()}`);
  }
  const path =
    action === 'check' ? '/api/run-check' : `/api/v1/courses/${encodeURIComponent(String(courseId))}/${action}`;
  return requestJson<CourseMutationResult>(path, {
    method: 'POST',
    headers,
    body: action === 'check' ? JSON.stringify({ course_id: Number(courseId) }) : '{}',
  });
}
