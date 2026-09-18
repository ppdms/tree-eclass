import type { LoaderFunctionArgs } from 'react-router';
import { fetchJson } from '@/lib/api';
import type {
  ChangeDetailPayload,
  CourseDetailPayload,
  CoursesCoverage,
  ExercisesPayload,
  InboxPayload,
  NavigationCourses,
  StudyPayload,
} from '@/lib/types';
import type { AskBootstrap } from '@/features/ask/types';
import type { SettingsPayload } from '@/features/settings/types';

const INITIAL_ACTIVITY_LIMIT = 10;

export function loadActivity({ request }: Pick<LoaderFunctionArgs, 'request'>): Promise<InboxPayload> {
  return fetchJson<InboxPayload>(`api/v1/inbox?include_timeline=false&limit=${INITIAL_ACTIVITY_LIMIT}&offset=0`, {
    signal: request.signal,
  });
}

export function loadCourses({ request }: Pick<LoaderFunctionArgs, 'request'>): Promise<CoursesCoverage> {
  return fetchJson<CoursesCoverage>('api/v1/courses/coverage', { signal: request.signal });
}

export function loadNavigationCourses({ request }: Pick<LoaderFunctionArgs, 'request'>): Promise<NavigationCourses> {
  return fetchJson<NavigationCourses>('api/v1/navigation/courses', { signal: request.signal }).catch((error) => {
    if (request.signal.aborted) throw error;
    return { courses: [] };
  });
}

export function loadCourseDetail(courseId: string, signal: AbortSignal): Promise<CourseDetailPayload> {
  return fetchJson<CourseDetailPayload>(`api/v1/courses/${encodeURIComponent(courseId)}/overview`, { signal });
}

export function loadChangeDetail(
  courseId: string,
  changeNo: string,
  signal: AbortSignal,
): Promise<ChangeDetailPayload> {
  return fetchJson<ChangeDetailPayload>(
    `api/courses/${encodeURIComponent(courseId)}/changes/${encodeURIComponent(changeNo)}`,
    { signal },
  );
}

export function loadStudy(signal: AbortSignal, courseId?: string): Promise<StudyPayload> {
  const suffix = courseId ? `?course_id=${encodeURIComponent(courseId)}` : '';
  return fetchJson<StudyPayload>(`api/v1/study${suffix}`, { signal });
}

export function loadExercises({ request }: Pick<LoaderFunctionArgs, 'request'>): Promise<ExercisesPayload> {
  return fetchJson<ExercisesPayload>('api/v1/exercises?include_ignored=true&include_details=false', {
    signal: request.signal,
  });
}

export function loadSettings({ request }: Pick<LoaderFunctionArgs, 'request'>): Promise<SettingsPayload> {
  return fetchJson<SettingsPayload>('api/v1/settings', { signal: request.signal });
}

export function loadAsk(conversationId: string | null, signal: AbortSignal): Promise<AskBootstrap> {
  const suffix = conversationId ? `?conversation_id=${encodeURIComponent(conversationId)}` : '';
  return fetchJson<AskBootstrap>(`api/ask/bootstrap${suffix}`, { signal });
}

export function courseLoader({ params, request }: LoaderFunctionArgs) {
  return loadCourseDetail(params.courseId!, request.signal);
}
export function changeLoader({ params, request }: LoaderFunctionArgs) {
  return loadChangeDetail(params.courseId!, params.changeNo!, request.signal);
}
export function studyLoader({ request }: LoaderFunctionArgs) {
  return loadStudy(request.signal, new URL(request.url).searchParams.get('course_id') || undefined);
}
export function askLoader({ request }: LoaderFunctionArgs) {
  return loadAsk(new URL(request.url).searchParams.get('c'), request.signal);
}
