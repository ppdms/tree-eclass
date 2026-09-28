import { navigate } from '@/app/navigation';
import * as React from 'react';
import { z } from 'zod/v4';
import type { CourseSummary } from '@/lib/types';

const errorBodySchema = z
  .object({
    detail: z.unknown(),
  })
  .partial();

const detailItemSchema = z
  .object({
    msg: z.string(),
  })
  .partial();

async function responseMessage(response: Response, fallback: string): Promise<string | null> {
  if (response.ok) return null;
  try {
    const parsed = errorBodySchema.safeParse(await response.json());
    if (!parsed.success) return fallback;
    const detail = parsed.data.detail;
    if (Array.isArray(detail)) {
      const messages = detail.map((item) => {
        const parsedItem = detailItemSchema.safeParse(item);
        return parsedItem.success && parsedItem.data.msg ? parsedItem.data.msg : String(item);
      });
      return messages.join('. ');
    }
    return String(detail || fallback);
  } catch {
    return fallback;
  }
}

async function renameCourse(form: HTMLFormElement, courseId: string | number): Promise<string | null> {
  const name = String(new FormData(form).get('name') || '').trim();
  const response = await fetch(`/api/v1/courses/${encodeURIComponent(String(courseId))}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({ name }),
  });
  return responseMessage(response, 'Could not rename this course.');
}

async function runCourseAction(action: string, courseId: string | number): Promise<string | null> {
  if (action === 'check') {
    const id = Number(courseId);
    const response = await fetch('/api/run-check', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ course_id: id }),
    });
    return responseMessage(response, 'Could not check this course.');
  }
  const headers = new Headers({ 'Content-Type': 'application/json', Accept: 'application/json' });
  if (action === 'delete' || action === 'reset') {
    headers.set('X-Tree-Eclass-Confirmation', `${action}:${courseId}`);
    headers.set('X-Idempotency-Key', globalThis.crypto?.randomUUID?.() || `${action}:${courseId}:${Date.now()}`);
  }
  const response = await fetch(`/api/v1/courses/${courseId}/${action}`, {
    method: 'POST',
    headers,
    body: '{}',
  });
  return responseMessage(response, `Could not ${action} this course.`);
}

export interface CourseActionFeedback {
  type: 'error';
  message: string;
}

export interface UseCourseActionsResult {
  busy: boolean;
  feedback: CourseActionFeedback | null;
  confirmation: { action: string; message: string; target: string } | null;
  rename: (event: React.FormEvent<HTMLFormElement>) => Promise<void>;
  runAction: () => Promise<void>;
  askAction: (action: string, message: string, target: string) => void;
  cancelConfirmation: () => void;
}

export function useCourseActions(course: CourseSummary): UseCourseActionsResult {
  const [busy, setBusy] = React.useState(false);
  const [feedback, setFeedback] = React.useState<CourseActionFeedback | null>(null);
  const [confirmation, setConfirmation] = React.useState<{
    action: string;
    message: string;
    target: string;
  } | null>(null);
  const rename = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setBusy(true);
    setFeedback(null);
    try {
      const error = await renameCourse(event.currentTarget, course.id);
      if (error) throw new Error(error);
      window.location.reload();
    } catch (error) {
      setFeedback({
        type: 'error',
        message: error instanceof Error ? error.message : 'Could not rename this course.',
      });
      setBusy(false);
    }
  };
  const runAction = async () => {
    if (!confirmation) return;
    const { action, target } = confirmation;
    setConfirmation(null);
    setBusy(true);
    setFeedback(null);
    try {
      const error = await runCourseAction(action, course.id);
      if (error) throw new Error(error);
      navigate(target);
    } catch (error) {
      setFeedback({
        type: 'error',
        message: error instanceof Error ? error.message : `Could not ${action} this course.`,
      });
      setBusy(false);
    }
  };
  const askAction = (action: string, message: string, target: string) => setConfirmation({ action, message, target });
  const cancelConfirmation = () => setConfirmation(null);
  return { busy, feedback, confirmation, rename, runAction, askAction, cancelConfirmation };
}
