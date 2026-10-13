import { useEffect, useMemo, useRef, useState } from 'react';
import type { Dispatch, MutableRefObject, SetStateAction } from 'react';
import { requestJson } from '../app/api';
import { isRevisionConflict } from '../roadmap/resource';
import { semanticId } from '../roadmap/styles';
import { buildCoursePracticeAttempt } from './attempt';
import type { CoursePracticeQuestion, PracticeAttemptBody, PracticeOutcome, RecordedPracticeAttempt } from './model';

interface RecorderProps {
  courseId: string | number;
  question?: CoursePracticeQuestion;
  revealed: boolean;
  note: string;
  stale: boolean;
  generation: number;
  fresh: boolean;
  onStarted: () => void;
  onCommitted: (saved: RecordedPracticeAttempt, question: CoursePracticeQuestion) => void;
  commitDraft: () => void;
}

interface AttemptIntent {
  payload: PracticeAttemptBody;
  commitDraft: () => void;
}

interface RejectedAttempt {
  generation: number;
  identity: string;
}

interface RecordingState {
  intents: MutableRefObject<Map<string, AttemptIntent>>;
  locked: MutableRefObject<boolean>;
  rejected: MutableRefObject<RejectedAttempt | null>;
  latestGeneration: MutableRefObject<number>;
  setBusy: Dispatch<SetStateAction<boolean>>;
  setError: Dispatch<SetStateAction<string | null>>;
  setConflict: Dispatch<SetStateAction<boolean>>;
}

function practiceAttemptIdentity(question: CoursePracticeQuestion, outcome: PracticeOutcome): string {
  return semanticId(
    question.blueprint_revision_id,
    question.unit_key,
    question.set_hash,
    question.question_id || question.id,
    outcome,
  );
}

async function recordAttempt(props: RecorderProps, state: RecordingState, outcome: PracticeOutcome): Promise<void> {
  const question = props.question;
  if (!question || !props.revealed || state.locked.current || props.stale || state.rejected.current) return;
  const identity = practiceAttemptIdentity(question, outcome);
  const intent = state.intents.current.get(identity) || {
    payload: buildCoursePracticeAttempt(props.courseId, question, props.revealed, props.note, outcome),
    commitDraft: props.commitDraft,
  };
  const payload = intent.payload;
  if (!payload.question_id) {
    state.setError('This question has no current identity. Reopen practice.');
    return;
  }
  state.intents.current.set(identity, intent);
  state.locked.current = true;
  state.setBusy(true);
  state.setError(null);
  props.onStarted();
  let saved: RecordedPracticeAttempt;
  try {
    saved = await requestJson<RecordedPracticeAttempt>('/api/study/practice/attempt', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch (failure) {
    const parsed = failure instanceof Error ? failure : new Error('That attempt could not be recorded.');
    state.setError(parsed.message);
    if (isRevisionConflict(parsed)) {
      state.rejected.current = { generation: state.latestGeneration.current, identity };
      state.setConflict(true);
    }
    return;
  } finally {
    state.locked.current = false;
    state.setBusy(false);
  }
  state.intents.current.delete(identity);
  props.onCommitted(saved, question);
  intent.commitDraft();
}

export function useAttemptRecorder(props: RecorderProps) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [conflict, setConflict] = useState(false);
  const rejected = useRef<RejectedAttempt | null>(null);
  const latestGeneration = useRef(props.generation);
  latestGeneration.current = props.generation;
  const intents = useRef(new Map<string, AttemptIntent>());
  const locked = useRef(false);
  useEffect(() => {
    const rejection = rejected.current;
    if (!rejection || !props.fresh || props.generation <= rejection.generation) return;
    // A new admitted read invalidates only the conflicting intent, not ordinary retries.
    intents.current.delete(rejection.identity);
    rejected.current = null;
    setConflict(false);
    setError(null);
  }, [props.generation, props.fresh]);
  const state = useMemo(
    () => ({ intents, locked, rejected, latestGeneration, setBusy, setError, setConflict }),
    [intents, locked, rejected, latestGeneration, setBusy, setError, setConflict],
  );
  const record = (outcome: PracticeOutcome) => recordAttempt(props, state, outcome);
  return { busy, error, conflict, record, hasRetry: intents.current.size > 0 };
}
