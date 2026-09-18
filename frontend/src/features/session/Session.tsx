/**
 * The study workspace.
 *
 * One blueprint action, the documents it cites, the analysis of the page in
 * view, the learner's own marks, and the unit's recall queue — in one place, so
 * that studying produces evidence as a side effect of happening rather than as
 * a form filled in afterwards.
 *
 * The workspace measures and records. It never decides: closing a session
 * reports the minutes it measured, but whether the action is done stays the
 * learner's call, the same boundary practice attempts already respect.
 *
 * The reader is the dominant element on screen: a top toolbar carries
 * identity/zoom/finish controls that used to be a permanently-docked rail,
 * and marks/insight/recall live in a drawer that only takes space when
 * actually opened (`SessionDrawer`). Selecting text acts where the selection
 * is (`SelectionToolbar`) rather than in a panel across the screen.
 */

import { errorLikeSchema } from '@/lib/errors';
import { LoadError, LoadingState } from './components/LoadStates';
import Workspace from './SessionWorkspace';
import { useSessionState } from './components/useSessionState';
import { useSessionControls } from './components/useReaderControls';

export interface SessionProps {
  courseId: string | number | null;
  actionId: string | null;
  documentId: string | null;
  initialPageNumber?: number;
}

export default function Session({ courseId, actionId, documentId, initialPageNumber = 1 }: SessionProps) {
  const state = useSessionState(courseId || '', actionId, documentId, initialPageNumber);
  const controls = useSessionControls(state);
  const { context, error } = state;

  if (error && !context) {
    const parsed = errorLikeSchema.safeParse(error);
    return <LoadError error={parsed.success ? parsed.data : { message: String(error) }} />;
  }
  if (!context) return <LoadingState />;

  return <Workspace {...state} courseId={courseId} {...controls} />;
}
