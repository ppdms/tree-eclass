import { getActivity, subscribeActivity } from '../app/activityState';
import { message } from './api';
import { SittingJournal } from './journal';
import type { Sitting } from './types';

export interface AttentionTarget {
  documentId: string;
  sourceHash: string;
  page: number;
  ready: boolean;
}
interface AttentionCallbacks {
  totals: (session: Sitting) => void;
  status: (paused: boolean, error: string | null) => void;
}
function activityNow(): number {
  return performance.timeOrigin + performance.now();
}
export class AttentionLedger {
  private target: AttentionTarget | null = null;
  private at = activityNow();
  private visible = getActivity().visible;
  private lastInput = this.at;
  private sending = Promise.resolve();
  private blocked = false;
  private error: string | null = null;
  private stopped = false;
  private holding = false;
  private unsubscribe: () => void;
  private timer: number;
  constructor(
    readonly journal: SittingJournal,
    private callbacks: AttentionCallbacks,
  ) {
    this.unsubscribe = subscribeActivity((event) => this.activity(event));
    this.timer = self.setInterval(() => {
      void this.flush().catch(() => undefined);
    }, 15000);
    this.report();
  }
  setTarget(target: AttentionTarget | null): void {
    const previous = this.target;
    if (
      previous?.documentId === target?.documentId &&
      previous?.sourceHash === target?.sourceHash &&
      previous?.page === target?.page &&
      previous?.ready === target?.ready
    )
      return;
    this.capture(activityNow());
    this.target = target;
    this.report();
    void this.drain().catch(() => undefined);
  }
  private activity(event: { kind: string; at: number; visible: boolean }): void {
    if (event.kind !== 'activity' || event.at - this.lastInput >= 90000) this.capture(event.at);
    if (event.kind === 'activity') this.lastInput = Math.max(this.lastInput, event.at);
    if (event.kind !== 'activity') this.visible = event.visible && event.kind !== 'pagehide';
    this.report();
    if (event.kind !== 'activity') void this.drain().catch(() => undefined);
  }
  private capture(now: number): void {
    now = Math.max(now, this.at);
    const start = Math.max(this.at, now - 15000);
    this.at = now;
    const target = this.target;
    const session = this.journal.state.sessionId;
    if (this.stopped || this.holding || this.blocked || !this.visible || !target?.ready || session === undefined)
      return;
    const cutoff = this.lastInput + 90000;
    this.append(start, Math.min(now, cutoff), true, target, session);
    this.append(Math.max(start, cutoff), now, false, target, session);
  }
  private append(start: number, end: number, active: boolean, target: AttentionTarget, session: Sitting['id']): void {
    const seconds = Math.floor((end - start) / 1000);
    if (seconds <= 0) return;
    const beat = {
      session_id: session,
      document_id: target.documentId,
      source_hash: target.sourceHash,
      page_number: target.page,
      interval_seconds: seconds,
      active,
    };
    const stored = this.journal.enqueue(beat).catch((error) => {
      this.fail(error);
    });
    this.sending = this.sending.then(() => stored);
  }
  private report(): void {
    const paused =
      this.stopped ||
      this.holding ||
      this.blocked ||
      !this.visible ||
      !this.target?.ready ||
      activityNow() - this.lastInput >= 90000;
    this.callbacks.status(paused, this.error);
  }
  private fail(cause: unknown): void {
    this.blocked = true;
    this.error = `Attention has not been confirmed: ${message(cause)} Retry pending time.`;
    this.callbacks.status(true, this.error);
  }
  drain(): Promise<void> {
    const operation = this.sending.then(async () => {
      if (this.blocked) throw new Error('Pending attention must be retried before continuing.');
      await this.journal.flushPending(this.callbacks.totals);
    });
    this.sending = operation.catch((error) => {
      this.fail(error);
    });
    return operation;
  }
  async flush(): Promise<void> {
    this.capture(activityNow());
    await this.drain();
    this.report();
  }
  async retry(): Promise<void> {
    this.blocked = false;
    this.error = null;
    this.at = activityNow();
    await this.drain();
    this.report();
  }
  async discardPending(): Promise<void> {
    try {
      await this.sending;
      await this.journal.discardPending();
      this.blocked = false;
      this.error = null;
      this.at = activityNow();
      this.report();
    } catch (failure) {
      this.fail(failure);
      throw failure;
    }
  }
  holdForFinish(): void {
    this.capture(activityNow());
    this.holding = true;
    this.report();
  }
  close(): void {
    this.stopped = true;
    self.clearInterval(this.timer);
    this.unsubscribe();
    this.report();
  }
  dispose(): void {
    this.capture(activityNow());
    void this.drain().catch(() => undefined);
    this.close();
  }
}
