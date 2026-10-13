import { useRef, useState } from 'react';
import { useNavigate } from 'react-router';
import type { CourseSummary } from '@/lib/types';
import { reloadPage } from '../app/browser';
import { publishCatalogChanged } from '../app/events';
import {
  courseConfirmation,
  renameCourse,
  runCourseAction,
  type CourseAction,
  type CourseConfirmation,
} from './actions';

export function useCourseActions(course: CourseSummary) {
  const navigate = useNavigate();
  const [busy, setBusy] = useState(false);
  const [feedback, setFeedback] = useState<string | null>(null);
  const [confirmation, setConfirmation] = useState<CourseConfirmation | null>(null);
  const pending = useRef(false);
  const submit = async (action: CourseAction | 'rename', name?: string) => {
    if (pending.current) return;
    if (action === 'rename' && !name?.trim()) {
      setFeedback('Enter a course name.');
      return;
    }
    setConfirmation(null);
    pending.current = true;
    setBusy(true);
    setFeedback(null);
    try {
      if (action === 'rename') await renameCourse(course.id, name || '');
      else await runCourseAction(action, course.id);
      if (action !== 'check') publishCatalogChanged();
      if (action === 'rename') reloadPage();
      // A successful check only enqueues work; navigation does not claim synchronization has finished.
      else void navigate(courseConfirmation(course, action).target);
    } catch (error) {
      setFeedback(error instanceof Error ? error.message : `Could not ${action} this course.`);
      pending.current = false;
      setBusy(false);
    }
  };
  return {
    busy,
    feedback,
    confirmation,
    rename: (name: string) => {
      void submit('rename', name);
    },
    runAction: () => {
      if (confirmation) void submit(confirmation.action);
    },
    askAction: (action: CourseAction) => {
      if (!pending.current) setConfirmation(courseConfirmation(course, action));
    },
    cancelConfirmation: () => setConfirmation(null),
  };
}
