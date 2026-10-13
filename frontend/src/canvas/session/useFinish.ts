import { useEffect, useRef, useState, type Dispatch, type MutableRefObject, type SetStateAction } from 'react';
import { api, codePointLength, message } from './api';
import type { FinishIntent } from './journal';
import type { FinishResult, Id, Outcome } from './types';
import type { SittingState } from './useSitting';

export interface FinishState {
  result: FinishResult | null;
  busy: boolean;
  error: string | null;
  warning: string | null;
  intent: FinishIntent | null;
  finish: (outcome: Outcome, note: string, confidence: string) => Promise<void>;
  retry: () => Promise<void>;
}
interface FinishDraftState {
  result: FinishResult | null;
  busy: boolean;
  error: string | null;
  warning: string | null;
  intent: FinishIntent | null;
  setResult: Dispatch<SetStateAction<FinishResult | null>>;
  setBusy: Dispatch<SetStateAction<boolean>>;
  setError: Dispatch<SetStateAction<string | null>>;
  setWarning: Dispatch<SetStateAction<string | null>>;
  setIntent: Dispatch<SetStateAction<FinishIntent | null>>;
  lock: MutableRefObject<boolean>;
  controller: MutableRefObject<AbortController>;
}
function useFinishDraft(sitting: SittingState): FinishDraftState {
  const [result, setResult] = useState<FinishResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [warning, setWarning] = useState<string | null>(null);
  const [intent, setIntent] = useState<FinishIntent | null>(null);
  const lock = useRef(false);
  const [initialController] = useState(() => new AbortController());
  const controller = useRef(initialController);
  const sessionId = sitting.session?.id;
  useEffect(() => {
    if (controller.current.signal.aborted) controller.current = new AbortController();
    const lifetime = controller.current;
    return () => lifetime.abort();
  }, []);
  useEffect(() => {
    setIntent(sitting.journal?.state.finish ?? null);
  }, [sitting.journal, sitting.session]);
  useEffect(() => {
    setResult(null);
    setError(null);
    setWarning(null);
  }, [sessionId]);
  return {
    result,
    busy,
    error,
    warning,
    intent,
    setResult,
    setBusy,
    setError,
    setWarning,
    setIntent,
    lock,
    controller,
  };
}
function finishSender(sitting: SittingState, state: FinishDraftState) {
  const { lock, controller, setBusy, setError, setIntent, setResult, setWarning } = state;
  return async (payload: FinishIntent) => {
    if (lock.current || !sitting.journal || !sitting.ledger || controller.current.signal.aborted) return;
    lock.current = true;
    setBusy(true);
    setError(null);
    setIntent(payload);
    sitting.ledger.holdForFinish();
    let sent = false;
    try {
      await sitting.journal.update((state) => ({ ...state, finish: payload }));
      await sitting.ledger.flush();
      controller.current.signal.throwIfAborted();
      sent = true;
      const saved = await api.finish(payload, controller.current.signal);
      sitting.ledger.close();
      setResult(saved);
      try {
        await sitting.journal.clear();
      } catch {
        setWarning('Outcome recorded. Browser resume-key cleanup was not confirmed.');
      }
    } catch (failure) {
      if (!controller.current.signal.aborted)
        setError(`${sent ? 'Outcome save unconfirmed' : 'Finish not sent'}: ` + message(failure));
    } finally {
      lock.current = false;
      if (!controller.current.signal.aborted) setBusy(false);
    }
  };
}
export function useFinish(sitting: SittingState): FinishState {
  const state = useFinishDraft(sitting);
  const { result, busy, error, warning, intent, setError } = state;
  const send = finishSender(sitting, state);
  const finish = async (outcome: Outcome, note: string, confidence: string) => {
    if (!sitting.session) return;
    try {
      await send(sitting.journal?.state.finish ?? newFinish(sitting.session.id, outcome, note, confidence));
    } catch (failure) {
      setError(message(failure));
    }
  };
  return {
    result,
    busy,
    error,
    warning,
    intent,
    finish,
    retry: async () => {
      if (intent) await send(intent);
    },
  };
}
function newFinish(sessionId: Id, outcome: Outcome, note: string, confidence: string): FinishIntent {
  if (
    codePointLength(note.trim()) > 4000 ||
    (confidence !== '' && (!Number.isInteger(Number(confidence)) || Number(confidence) < 0 || Number(confidence) > 5))
  ) {
    throw new Error('Reflection must fit 4000 characters; confidence must be a whole number from 0 to 5.');
  }
  return {
    session_id: sessionId,
    outcome,
    note: note.trim() || null,
    confidence: confidence === '' ? null : Number(confidence),
  };
}
