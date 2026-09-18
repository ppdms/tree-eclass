/**
 * The attention ledger's client half.
 *
 * The whole point of studying inside the app is that time no longer has to be
 * remembered and typed in afterwards. That only works if the measurement is
 * trustworthy, which means being conservative on purpose:
 *
 * - a hidden tab is not reading;
 * - a focused tab with no interaction for a while is not reading either — that
 *   is a coffee break with a PDF open;
 * - a beat is attributed to the page actually on screen, not to the document as
 *   a whole, so "which pages did I really cover" has a real answer.
 *
 * Beats carry a monotonic sequence number. The server counts each sequence once,
 * so a retried beat after a lost response cannot inflate the total. Under-
 * counting is the acceptable failure here; over-counting is not, because a
 * flattering number is worse than no number.
 */

import { useCallback, useEffect, useRef, useState } from 'react';

import { api } from './api';
import type { SessionInfo } from '@/features/session/reader/types';

/** How often a beat is sent. Also the maximum a single beat can contribute. */
export const BEAT_SECONDS = 15;

/** Silence longer than this means the learner stepped away, not that they read slowly. */
export const IDLE_SECONDS = 90;

export type UseAttentionOnTotals = (session: SessionInfo) => void;

export interface UseAttentionArgs {
  sessionId: string | number | null | undefined;
  documentId: string | number | null | undefined;
  pageNumber?: number;
  enabled: boolean;
  onTotals?: UseAttentionOnTotals;
}

interface SendAttentionBeatArgs {
  state: {
    sessionId: string | number | null | undefined;
    documentId: string | number | null | undefined;
    pageNumber?: number;
    enabled: boolean;
  };
  force: boolean;
  sequence: { current: number };
  lastBeatAt: { current: number };
  pending: { current: boolean };
  lastInteraction: { current: number };
  setActiveSeconds: (value: number) => void;
  setIdle: (value: boolean) => void;
  onTotals?: UseAttentionOnTotals;
}

async function sendAttentionBeat({
  state,
  force,
  sequence,
  lastBeatAt,
  pending,
  lastInteraction,
  setActiveSeconds,
  setIdle,
  onTotals,
}: SendAttentionBeatArgs): Promise<void> {
  if (!state.enabled || !state.sessionId || !state.documentId) return;
  if (pending.current && !force) return;

  const now = Date.now();
  const elapsed = Math.round((now - lastBeatAt.current) / 1000);
  if (elapsed <= 0) return;

  const visible = document.visibilityState === 'visible';
  const interacted = (now - lastInteraction.current) / 1000 < IDLE_SECONDS;
  const active = visible && interacted;
  setIdle(!active);

  lastBeatAt.current = now;
  pending.current = true;
  const seq = sequence.current++;
  try {
    const result = await api.heartbeat({
      session_id: state.sessionId,
      sequence: seq,
      document_id: state.documentId,
      page_number: state.pageNumber || 1,
      active,
      interval_seconds: Math.min(elapsed, BEAT_SECONDS),
    });
    if (result?.session) {
      setActiveSeconds(result.session.active_seconds || 0);
      onTotals?.(result.session);
    }
  } catch {
    // A dropped beat costs at most one interval. Re-sending it under
    // a new sequence number would be the one way to corrupt the
    // ledger, so it is simply let go.
  } finally {
    pending.current = false;
  }
}

interface UseLedgerHeartbeatArgs {
  enabled: boolean;
  sessionId: string | number | null | undefined;
  sendBeat: (force?: boolean) => Promise<void>;
  lastBeatAt: { current: number };
}

function useLedgerHeartbeat({ enabled, sessionId, sendBeat, lastBeatAt }: UseLedgerHeartbeatArgs): void {
  useEffect(() => {
    if (!enabled || !sessionId) return undefined;
    lastBeatAt.current = Date.now();
    const timer = setInterval(() => {
      void sendBeat();
    }, BEAT_SECONDS * 1000);

    // Leaving the tab banks whatever has accrued rather than discarding it.
    const onHide = (): void => {
      if (document.visibilityState === 'hidden') {
        void sendBeat(true);
      }
    };
    document.addEventListener('visibilitychange', onHide);
    window.addEventListener('pagehide', onHide);
    return () => {
      clearInterval(timer);
      document.removeEventListener('visibilitychange', onHide);
      window.removeEventListener('pagehide', onHide);
    };
  }, [enabled, sessionId, sendBeat, lastBeatAt]);
}

interface UseAttentionLedgerArgs {
  sessionId: string | number | null | undefined;
  documentId: string | number | null | undefined;
  pageNumber?: number;
  enabled: boolean;
  onTotals?: UseAttentionOnTotals;
  setActiveSeconds: (value: number) => void;
  setIdle: (value: boolean) => void;
  lastInteraction: { current: number };
}

export interface UseAttentionLedgerResult {
  flush: () => Promise<void>;
}

export function useAttentionLedger({
  sessionId,
  documentId,
  pageNumber,
  enabled,
  onTotals,
  setActiveSeconds,
  setIdle,
  lastInteraction,
}: UseAttentionLedgerArgs): UseAttentionLedgerResult {
  const sequence = useRef(0);
  const lastBeatAt = useRef(Date.now());
  const pending = useRef(false);
  const latest = useRef({ sessionId, documentId, pageNumber, enabled });

  latest.current = { sessionId, documentId, pageNumber, enabled };

  const sendBeat = useCallback(
    (force = false): Promise<void> =>
      sendAttentionBeat({
        state: latest.current,
        force,
        sequence,
        lastBeatAt,
        pending,
        lastInteraction,
        setActiveSeconds,
        setIdle,
        onTotals,
      }),
    [onTotals, setActiveSeconds, setIdle, lastInteraction],
  );

  useLedgerHeartbeat({ enabled, sessionId, sendBeat, lastBeatAt });

  // A page change closes out the previous page's interval immediately, so the
  // per-page ledger reflects where the time was really spent.
  useEffect(() => {
    if (enabled && sessionId) {
      void sendBeat(true);
    }
  }, [pageNumber, documentId, enabled, sessionId, sendBeat]);

  return { flush: () => sendBeat(true) };
}

export interface UseAttentionResult {
  activeSeconds: number;
  idle: boolean;
  flush: () => Promise<void>;
}

export function useAttention({
  sessionId,
  documentId,
  pageNumber,
  enabled,
  onTotals,
}: UseAttentionArgs): UseAttentionResult {
  const [activeSeconds, setActiveSeconds] = useState(0);
  const [idle, setIdle] = useState(false);

  const lastInteraction = useRef(Date.now());
  const ledger = useAttentionLedger({
    sessionId,
    documentId,
    pageNumber,
    enabled,
    onTotals,
    setActiveSeconds,
    setIdle,
    lastInteraction,
  });

  // Any of these means a person is present. Scroll and selection matter most
  // for reading, where minutes can pass between clicks.
  useEffect(() => {
    const seen = (): void => {
      lastInteraction.current = Date.now();
      setIdle(false);
    };
    const events = ['pointerdown', 'pointermove', 'keydown', 'wheel', 'scroll', 'selectionchange'];
    events.forEach((name) => document.addEventListener(name, seen, { passive: true }));
    return () => events.forEach((name) => document.removeEventListener(name, seen));
  }, [setIdle]);

  return { activeSeconds, idle, flush: ledger.flush };
}
