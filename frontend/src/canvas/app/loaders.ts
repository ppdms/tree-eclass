// Route requests begin with route matching, not after the feature's rendering chunk has downloaded.
import { redirect, type LoaderFunctionArgs } from 'react-router';
import type {
  ChangeDetailPayload,
  CourseDetailPayload,
  CoursesCoverage,
  ExercisesPayload,
  StudyPayload,
} from '@/lib/types';
import { nextCourseSequence, type CoursesCatalogData } from '../courses/orderState';
import { nextExercisesOrdinal, type ExercisesRouteData } from '../exercises/freshness';
import { exercisesPath } from '../exercises/model';
import { studyPath } from '../study/model';
import { fetchSettings, type SettingsLoad } from '../settings/data';
import type { SessionQuery } from '../session/types';
import type { AskBootstrap } from '../ask/model';
import { requestJson } from './api';

export async function loadCourses({ request }: LoaderFunctionArgs): Promise<CoursesCatalogData> {
  const requestSequence = nextCourseSequence();
  const coverage = await requestJson<CoursesCoverage>('/api/v1/courses/coverage', { signal: request.signal });
  return { ...coverage, requestSequence };
}

export function loadCourseDetail({ params, request }: LoaderFunctionArgs): Promise<CourseDetailPayload> {
  return requestJson<CourseDetailPayload>(`/api/v1/courses/${encodeURIComponent(params.courseId!)}/overview`, {
    signal: request.signal,
  });
}

export async function loadExercises({ request }: LoaderFunctionArgs): Promise<ExercisesRouteData> {
  const readStarted = nextExercisesOrdinal();
  const data = await requestJson<ExercisesPayload>(exercisesPath, { signal: request.signal });
  return { ...data, readStarted };
}

export function loadStudy({ request }: LoaderFunctionArgs): Promise<StudyPayload> {
  return requestJson<StudyPayload>(studyPath(new URL(request.url).searchParams.get('course_id')), {
    signal: request.signal,
  });
}

export interface ChangeRouteData {
  courseId: string;
  changeNo: string;
  detail: ChangeDetailPayload;
}
export async function loadChange({ params, request }: LoaderFunctionArgs): Promise<ChangeRouteData> {
  const courseId = params.courseId || '';
  const changeNo = params.changeNo || '';
  const path = `/api/courses/${encodeURIComponent(courseId)}/changes/${encodeURIComponent(changeNo)}`;
  const detail = await requestJson<ChangeDetailPayload>(path, { signal: request.signal });
  request.signal.throwIfAborted();
  return { courseId, changeNo, detail };
}

export function loadSettings({ request }: LoaderFunctionArgs): Promise<SettingsLoad> {
  return fetchSettings(request.signal);
}

export function loadSessionQuery({ request }: LoaderFunctionArgs): SessionQuery {
  const params = new URL(request.url).searchParams;
  if (!params.has('course_id')) throw redirect('/study');
  const requested = Number(params.get('page'));
  const courseId = String(Number(params.get('course_id')) || '');
  const actionId = params.get('action_id');
  const documentId = params.get('document_id');
  return {
    courseId,
    actionId,
    documentId,
    page: Number.isInteger(requested) && requested > 0 ? requested : 1,
    identity: JSON.stringify([params.get('course_id'), actionId, documentId]),
  };
}

export function loadAskBootstrap(query: string, signal?: AbortSignal): Promise<AskBootstrap> {
  const conversationId = new URLSearchParams(query).get('c');
  const suffix = conversationId ? `?conversation_id=${encodeURIComponent(conversationId)}` : '';
  return requestJson<AskBootstrap>(`/api/ask/bootstrap${suffix}`, { signal });
}
export function loadAsk({ request }: LoaderFunctionArgs): Promise<AskBootstrap> {
  return loadAskBootstrap(new URL(request.url).search, request.signal);
}
