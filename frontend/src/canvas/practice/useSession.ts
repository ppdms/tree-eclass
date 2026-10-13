import { useMemo } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { matchingPracticeView } from './model';
import type { CoursePracticeQuestion, CoursePracticeView, PracticeOutcome } from './model';
import { useAttemptRecorder } from './useAttemptRecorder';
import { usePracticeDraft } from './useDraft';
import { usePracticePublication } from './usePublication';

export interface PracticeSession {
  index: number;
  revealed: boolean;
  note: string;
  queue: CoursePracticeQuestion[];
  status: string;
  refreshError: string | null;
  busy: boolean;
  error: string | null;
  conflict: boolean;
  hasRetry: boolean;
  stale: boolean;
  record: (outcome: PracticeOutcome) => Promise<void>;
  setRevealed: Dispatch<SetStateAction<boolean>>;
  setNote: Dispatch<SetStateAction<string>>;
  next: () => void;
}

function usePracticeQueue(view: CoursePracticeView | null) {
  return useMemo(
    () =>
      (view?.units || []).flatMap((unit) =>
        (unit.questions || []).map((question) => ({
          ...question,
          unit_title: unit.title || question.unit_title,
          unit_key: question.unit_key || unit.unit_key,
        })),
      ),
    [view],
  );
}

/** Question drafts, admitted read generations, and retry intents have independent lifetimes. */
export function usePracticeSession(
  courseId: string | number,
  view: CoursePracticeView | null,
  setView: Dispatch<SetStateAction<CoursePracticeView | null>>,
  revision: string | undefined,
  generation: number,
  current: boolean,
): PracticeSession {
  const queue = usePracticeQueue(view);
  const draft = usePracticeDraft(queue);
  const fresh = current && matchingPracticeView(view, courseId, revision);
  const publication = usePracticePublication(view, setView, generation, fresh, revision);
  const recorder = useAttemptRecorder({
    courseId,
    question: queue[draft.index],
    revealed: draft.revealed,
    note: draft.note,
    stale: publication.stale || view?.enabled === false,
    generation,
    fresh,
    onStarted: publication.onStarted,
    onCommitted: publication.onCommitted,
    commitDraft: draft.committed,
  });
  return {
    index: draft.index,
    revealed: draft.revealed,
    note: draft.note,
    queue,
    status: publication.status,
    refreshError: publication.refreshError,
    ...recorder,
    stale: publication.stale || recorder.conflict,
    setRevealed: draft.setRevealed,
    setNote: draft.setNote,
    next: draft.next,
  };
}
