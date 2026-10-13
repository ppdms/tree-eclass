import { useEffect, useState, type Dispatch, type SetStateAction } from 'react';
import { api, message } from './api';
import { AttentionLedger } from './attention';
import { openSittingJournal, SittingJournal } from './journal';
import type { Context, Sitting } from './types';

export interface SittingState {
  session: Sitting | null;
  journal: SittingJournal | null;
  ledger: AttentionLedger | null;
  starting: boolean;
  error: string | null;
  attentionError: string | null;
  paused: boolean;
  retry: () => void;
  restart: () => Promise<void>;
}
interface SittingStartArgs {
  course: string;
  context: Context;
  identity: string;
  attempt: number;
}
interface SittingStartCallbacks {
  setJournal: Dispatch<SetStateAction<SittingJournal | null>>;
  setStarting: Dispatch<SetStateAction<boolean>>;
  setError: Dispatch<SetStateAction<string | null>>;
  setSession: Dispatch<SetStateAction<Sitting | null>>;
  setPaused: Dispatch<SetStateAction<boolean>>;
  setAttentionError: Dispatch<SetStateAction<string | null>>;
  setLedger: Dispatch<SetStateAction<AttentionLedger | null>>;
}
function useSittingStart(
  { course, context, identity, attempt }: SittingStartArgs,
  callbacks: SittingStartCallbacks,
): void {
  useEffect(() => {
    const controller = new AbortController();
    let attention: AttentionLedger | null = null;
    const next = openSittingJournal(course, identity);
    callbacks.setJournal(next);
    callbacks.setStarting(true);
    callbacks.setError(null);
    void startSitting(next, course, context, controller.signal)
      .then(async (result) => {
        if (controller.signal.aborted) return;
        callbacks.setSession(result);
        attention = createAttention(
          next,
          controller.signal,
          callbacks.setSession,
          callbacks.setPaused,
          callbacks.setAttentionError,
        );
        if (next.state.finish) attention.holdForFinish();
        callbacks.setLedger(attention);
        await attention.drain().catch(() => undefined);
        if (result.ended_at) attention.close();
      })
      .catch((failure) => {
        if (!controller.signal.aborted) callbacks.setError(message(failure));
      })
      .finally(() => {
        if (!controller.signal.aborted) callbacks.setStarting(false);
      });
    return () => {
      controller.abort();
      attention?.dispose();
    };
  }, [course, identity, context, attempt]);
}

export function useSitting(course: string, context: Context): SittingState {
  const [session, setSession] = useState<Sitting | null>(null);
  const [journal, setJournal] = useState<SittingJournal | null>(null);
  const [ledger, setLedger] = useState<AttentionLedger | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attentionError, setAttentionError] = useState<string | null>(null);
  const [paused, setPaused] = useState(true);
  const [starting, setStarting] = useState(true);
  const [attempt, setAttempt] = useState(0);
  const identity = context.action?.action_id || `doc:${context.documents[0]?.document_id ?? ''}`;
  useSittingStart(
    { course, context, identity, attempt },
    {
      setJournal,
      setStarting,
      setError,
      setSession,
      setPaused,
      setAttentionError,
      setLedger,
    },
  );
  const restart = async () => {
    try {
      ledger?.close();
      await journal?.clear();
      setSession(null);
      setLedger(null);
      setAttempt((value) => value + 1);
    } catch (failure) {
      setError(`New sitting not started: ${message(failure)}`);
    }
  };
  return {
    session,
    journal,
    ledger,
    starting,
    error,
    attentionError,
    paused,
    retry: () => {
      if (error && session?.ended_at) void restart();
      else if (error || !session || !ledger) setAttempt((value) => value + 1);
      else void ledger.retry().catch(() => undefined);
    },
    restart,
  };
}
function createAttention(
  journal: SittingJournal,
  signal: AbortSignal,
  setSession: Dispatch<SetStateAction<Sitting | null>>,
  setPaused: Dispatch<SetStateAction<boolean>>,
  setError: Dispatch<SetStateAction<string | null>>,
): AttentionLedger {
  return new AttentionLedger(journal, {
    totals: (value) => {
      if (!signal.aborted)
        setSession((current) =>
          current &&
          (current.active_seconds > value.active_seconds ||
            current.visible_seconds > value.visible_seconds ||
            (current.ended_at && !value.ended_at))
            ? current
            : value,
        );
    },
    status: (idle, failure) => {
      if (!signal.aborted) {
        setPaused(idle);
        setError(failure);
      }
    },
  });
}
async function startSitting(journal: SittingJournal, course: string, context: Context, signal: AbortSignal) {
  await journal.prepare();
  signal.throwIfAborted();
  const result = await api.start(
    {
      course_id: course,
      session_key: journal.state.key,
      action_id: context.action?.action_id || '',
      unit_key: context.unit_key || '',
      plan_revision: context.plan_revision || '',
      planned_minutes: context.action?.estimated_minutes ?? null,
    },
    signal,
  );
  await journal.bind(result.session.id, result.next_sequence);
  return result.session;
}
