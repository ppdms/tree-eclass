import { useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { practiceQuestionIdentity } from './model';
import type { CoursePracticeQuestion } from './model';
import { isStateUpdater } from './stateAction';

interface QuestionDraft {
  identity: string;
  revealed: boolean;
  note: string;
}

const emptyDraft: QuestionDraft = { identity: '', revealed: false, note: '' };

function questionDraft(identity: string, draft: QuestionDraft, setDraft: Dispatch<SetStateAction<QuestionDraft>>) {
  const revealed = draft.identity === identity && draft.revealed;
  const note = draft.identity === identity ? draft.note : '';
  const setRevealed: Dispatch<SetStateAction<boolean>> = (action) => {
    setDraft((current) => {
      const value = current.identity === identity ? current.revealed : false;
      return {
        identity,
        note: current.identity === identity ? current.note : '',
        revealed: isStateUpdater(action) ? action(value) : action,
      };
    });
  };
  const setNote: Dispatch<SetStateAction<string>> = (action) => {
    setDraft((current) => {
      const value = current.identity === identity ? current.note : '';
      return {
        identity,
        revealed: current.identity === identity && current.revealed,
        note: isStateUpdater(action) ? action(value) : action,
      };
    });
  };
  return { revealed, note, setRevealed, setNote };
}

export function usePracticeDraft(queue: CoursePracticeQuestion[]) {
  const [draft, setDraft] = useState(emptyDraft);
  const index = Math.max(
    0,
    queue.findIndex((question) => practiceQuestionIdentity(question) === draft.identity),
  );
  const controls = questionDraft(practiceQuestionIdentity(queue[index]), draft, setDraft);
  // The retry intent owns this exact draft, not whatever is active when it finishes.
  const committed = () => setDraft((current) => (current === draft ? emptyDraft : current));
  const next = () => {
    const identity = queue.length ? practiceQuestionIdentity(queue[(index + 1) % queue.length]) : '';
    setDraft({ identity, revealed: false, note: '' });
  };
  return { index, ...controls, committed, next };
}
