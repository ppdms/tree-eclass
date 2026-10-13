import { useEffect, useMemo, useRef, useState, type Dispatch, type MutableRefObject, type SetStateAction } from 'react';
import { api, codePointLength, message, requestKey } from './api';
import type { AttemptPayload, AttemptResult, Practice, PracticeQuestion, RecallOutcome } from './types';

export interface RecallState {
  practice: Practice | null;
  question: PracticeQuestion | undefined;
  count: number;
  loading: boolean;
  error: string | null;
  busy: boolean;
  answer: string;
  revealed: boolean;
  saved: AttemptResult | null;
  pending: AttemptPayload | null;
  setAnswer: (value: string) => void;
  reveal: () => void;
  refresh: () => void;
  record: (outcome: RecallOutcome) => Promise<void>;
  retrySave: () => Promise<void>;
}
interface RecallDraft {
  answer: string;
  revealed: boolean;
}
interface RecallClock {
  seconds(): number;
  reset(): void;
}
interface RecallSessionState extends Pick<
  RecallState,
  'practice' | 'loading' | 'error' | 'busy' | 'saved' | 'pending' | 'question' | 'count'
> {
  setPractice: Dispatch<SetStateAction<Practice | null>>;
  setError: Dispatch<SetStateAction<string | null>>;
  setBusy: Dispatch<SetStateAction<boolean>>;
  setSaved: Dispatch<SetStateAction<AttemptResult | null>>;
  setPending: Dispatch<SetStateAction<AttemptPayload | null>>;
  setDrafts: Dispatch<SetStateAction<Record<string, RecallDraft>>>;
  setAttempt: Dispatch<SetStateAction<number>>;
  drafts: Record<string, RecallDraft>;
  writes: MutableRefObject<AbortController>;
  intent: MutableRefObject<AttemptPayload | null>;
  lock: MutableRefObject<boolean>;
  clock: RecallClock;
}
function useRecallWrites(): MutableRefObject<AbortController> {
  const [initialController] = useState(() => new AbortController());
  const writes = useRef(initialController);
  useEffect(() => {
    if (writes.current.signal.aborted) writes.current = new AbortController();
    const lifetime = writes.current;
    return () => lifetime.abort();
  }, []);
  return writes;
}
function useRecallSession(course: string, unit: string, active: boolean): RecallSessionState {
  const [practice, setPractice] = useState<Practice | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [saved, setSaved] = useState<AttemptResult | null>(null);
  const [pending, setPending] = useState<AttemptPayload | null>(null);
  const [drafts, setDrafts] = useState<Record<string, RecallDraft>>({});
  const [attempt, setAttempt] = useState(0);
  const writes = useRecallWrites();
  const intent = useRef<AttemptPayload | null>(null);
  const lock = useRef(false);
  const { question, count } = useMemo(() => recallSummary(practice), [practice]);
  const clock = useRecallClock(active && Boolean(question) && saved?.practice !== null);
  useRecallRead({
    course,
    unit,
    active,
    attempt,
    practice,
    saved,
    setPractice,
    setLoading,
    setError,
    setSaved,
    clearRecorded: (id) => setDrafts((all) => ({ ...all, [String(id)]: { answer: '', revealed: false } })),
  });
  return {
    practice,
    loading,
    error,
    busy,
    saved,
    pending,
    drafts,
    question,
    count,
    writes,
    intent,
    lock,
    clock,
    setPractice,
    setError,
    setBusy,
    setSaved,
    setPending,
    setDrafts,
    setAttempt,
  };
}
interface RecallRecordArgs {
  course: string;
  id: string;
  draft: RecallDraft;
  question: PracticeQuestion | undefined;
  state: RecallSessionState;
  send: (payload: AttemptPayload) => Promise<void>;
}
function recallRecorder({ course, id, draft, question, state, send }: RecallRecordArgs) {
  return async (outcome: RecallOutcome) => {
    if (!id || !draft.revealed || state.saved?.practice === null || state.lock.current) return;
    if (codePointLength(draft.answer.trim()) > 20000) {
      state.setError('Your answer must fit 20000 characters. It has not been submitted.');
      return;
    }
    const payload = state.intent.current ?? {
      course_id: course,
      question_id: question?.question_id ?? question?.id ?? '',
      outcome,
      answer: draft.answer.trim() || null,
      confidence: null,
      seconds: state.clock.seconds(),
      idempotency_key: requestKey('recall'),
    };
    await send(payload);
  };
}
export function useRecall(course: string, unit: string, active: boolean): RecallState {
  const state = useRecallSession(course, unit, active);
  const { practice, loading, error, busy, saved, pending, question, count, drafts } = state;
  const id = String(question?.question_id ?? question?.id ?? '');
  const draft = drafts[id] ?? { answer: '', revealed: false };
  const change = (patch: Partial<RecallDraft>) =>
    state.setDrafts((all) => ({ ...all, [id]: { ...(all[id] ?? { answer: '', revealed: false }), ...patch } }));
  const send = recallSender({
    ...state,
    writes: state.writes.current,
    onRecorded: (clear) => {
      if (clear) change({ answer: '', revealed: false });
      state.clock.reset();
    },
  });
  const record = recallRecorder({ course, id, draft, question, state, send });
  return {
    practice,
    question,
    count,
    loading,
    error,
    busy,
    saved,
    pending,
    answer: draft.answer,
    revealed: draft.revealed,
    setAnswer: (value) => change({ answer: value }),
    reveal: () => change({ revealed: true }),
    refresh: () => state.setAttempt((value) => value + 1),
    record,
    retrySave: async () => {
      if (state.intent.current) await send(state.intent.current);
    },
  };
}
function recallSummary(practice: Practice | null) {
  let selected: PracticeQuestion | undefined;
  let unitTitle: string | undefined;
  let unitKey: string | undefined;
  let count = 0;
  for (const unit of practice?.units ?? []) {
    for (const question of unit.questions ?? []) {
      count++;
      if (selected && (question.queue_rank ?? 0) >= (selected.queue_rank ?? 0)) continue;
      selected = question;
      unitTitle = unit.title;
      unitKey = question.unit_key ?? unit.unit_key;
    }
  }
  return { count, question: selected ? { ...selected, unit_title: unitTitle, unit_key: unitKey } : undefined };
}
function useRecallClock(active: boolean): RecallClock {
  const clock = useRef({ elapsed: 0, opened: 0 });
  const running = useRef(active);
  running.current = active;
  useEffect(() => {
    if (active) clock.current.opened = performance.now();
    return () => {
      if (clock.current.opened) clock.current.elapsed += performance.now() - clock.current.opened;
      clock.current.opened = 0;
    };
  }, [active]);
  return {
    seconds: () =>
      Math.min(
        86400,
        Math.max(
          0,
          Math.round(
            (clock.current.elapsed + (clock.current.opened ? performance.now() - clock.current.opened : 0)) / 1000,
          ),
        ),
      ),
    reset: () => {
      clock.current = { elapsed: 0, opened: running.current ? performance.now() : 0 };
    },
  };
}
interface RecallReadArgs {
  course: string;
  unit: string;
  active: boolean;
  attempt: number;
  practice: Practice | null;
  saved: AttemptResult | null;
  clearRecorded: (id: AttemptPayload['question_id']) => void;
  setPractice: (value: Practice) => void;
  setLoading: (value: boolean) => void;
  setError: (value: string | null) => void;
  setSaved: (value: AttemptResult | null) => void;
}
function useRecallRead(args: RecallReadArgs): void {
  const last = useRef(-1);
  useEffect(() => {
    if (!args.active || (args.practice && last.current === args.attempt)) return;
    const controller = new AbortController();
    args.setLoading(true);
    args.setError(null);
    void api
      .practice(args.course, args.unit, controller.signal)
      .then((result) => {
        if (controller.signal.aborted) return;
        if (args.saved?.practice === null) args.clearRecorded(args.saved.attempt.question_id);
        last.current = args.attempt;
        args.setPractice(result);
        args.setSaved(null);
      })
      .catch((failure) => {
        if (!controller.signal.aborted) args.setError(message(failure));
      })
      .finally(() => {
        if (!controller.signal.aborted) args.setLoading(false);
      });
    return () => {
      controller.abort();
      args.setLoading(false);
    };
  }, [args.course, args.unit, args.active, args.attempt]);
}
interface SenderArgs {
  writes: AbortController;
  lock: { current: boolean };
  intent: { current: AttemptPayload | null };
  setPending: (value: AttemptPayload | null) => void;
  setBusy: (value: boolean) => void;
  setError: (value: string | null) => void;
  setSaved: (value: AttemptResult | null) => void;
  setPractice: (value: Practice) => void;
  onRecorded: (clear: boolean) => void;
}
function recallSender(args: SenderArgs) {
  return async (payload: AttemptPayload) => {
    if (args.lock.current || args.writes.signal.aborted) return;
    args.lock.current = true;
    args.intent.current = payload;
    args.setPending(payload);
    args.setBusy(true);
    args.setError(null);
    try {
      const result = await api.attempt(payload, args.writes.signal);
      if (args.writes.signal.aborted) return;
      args.setSaved(result);
      if (result.practice) args.setPractice(result.practice);
      args.intent.current = null;
      args.setPending(null);
      args.onRecorded(Boolean(result.practice));
    } catch (failure) {
      if (!args.writes.signal.aborted) args.setError(`Attempt save unconfirmed: ${message(failure)}`);
    } finally {
      args.lock.current = false;
      if (!args.writes.signal.aborted) args.setBusy(false);
    }
  };
}
