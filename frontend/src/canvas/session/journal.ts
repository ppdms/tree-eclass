import { sessionValue, setSessionValue } from '../app/preferences';
import { z } from 'zod/v4';
import { api, requestKey } from './api';
import type { Beat, Id, Outcome, Sitting } from './types';

const journals = new Map<string, SittingJournal>();

// Decode native-storage JSON once on restore. Loose records retain additional
// persisted fields; all valid beats/finish intents keep their exact text and numbers.
const idSchema = z.union([z.string(), z.number()]);
const beatSchema = z.looseObject({
  session_id: idSchema,
  sequence: z.number(),
  document_id: z.string(),
  source_hash: z.string(),
  page_number: z.number(),
  interval_seconds: z.number(),
  active: z.boolean(),
});
const finishSchema = z.looseObject({
  session_id: idSchema,
  outcome: z.enum(['completed', 'partial', 'stuck', 'deferred', 'abandoned']),
  note: z.string().nullable(),
  confidence: z.number().nullable(),
});
const journalSchema = z.looseObject({
  key: z.string(),
  sessionId: idSchema.optional(),
  nextSequence: z.number().refine(Number.isInteger),
  pending: z.array(beatSchema),
  finish: finishSchema.optional(),
});
export interface FinishIntent {
  session_id: Id;
  outcome: Outcome;
  note: string | null;
  confidence: number | null;
}
interface JournalState {
  key: string;
  sessionId?: Id;
  nextSequence: number;
  pending: Beat[];
  finish?: FinishIntent;
}
export class SittingJournal {
  readonly storageKey: string;
  readonly ledgerKey: string;
  state: JournalState;
  private writes = Promise.resolve();
  private heartbeats = Promise.resolve();
  private latestSession: Sitting | null = null;
  private readonly captured: Omit<Beat, 'sequence'>[] = [];
  constructor(course: string, identity: string) {
    this.storageKey = `study-session:${course}:${identity}`;
    this.ledgerKey = `study-session-ledger:${course}:${identity}`;
    const key = sessionValue(this.storageKey) ?? requestKey('sess');
    const raw = sessionValue(this.ledgerKey);
    this.state = { key, nextSequence: 0, pending: [] };
    if (!raw) return;
    try {
      const saved = journalSchema.safeParse(JSON.parse(raw));
      if (saved.success && saved.data.key === key) {
        this.state = saved.data;
      }
    } catch {
      /* The server watermark repairs a missing or unreadable local sequence record. */
    }
  }
  async prepare(): Promise<void> {
    await setSessionValue(this.storageKey, this.state.key);
    await this.update((state) => state);
  }
  update(change: (state: JournalState) => JournalState): Promise<void> {
    return this.write(() => this.save(change(this.state)));
  }
  private write(save: () => Promise<void>): Promise<void> {
    const operation = this.writes.then(save);
    this.writes = operation.catch(() => undefined);
    return operation;
  }
  private async save(next: JournalState): Promise<void> {
    await setSessionValue(this.ledgerKey, JSON.stringify(next));
    this.state = next;
  }
  async bind(sessionId: Id, nextSequence: number): Promise<void> {
    if (!Number.isInteger(nextSequence) || nextSequence < 0 || nextSequence > 2 ** 31) {
      throw new Error('The server did not provide a valid heartbeat resume sequence.');
    }
    await this.update((state) => {
      if (state.pending.some((beat) => String(beat.session_id) !== String(sessionId))) {
        throw new Error('Saved attention belongs to another sitting; it has not been replayed.');
      }
      const pendingNext = state.pending.reduce((value, beat) => Math.max(value, beat.sequence + 1), 0);
      return { ...state, sessionId, nextSequence: Math.max(nextSequence, state.nextSequence, pendingNext) };
    });
  }
  enqueue(beat: Omit<Beat, 'sequence'>): Promise<void> {
    // A rejected storage write must remain retryable, including after an in-app remount.
    this.captured.push({ ...beat });
    return this.persistCaptured();
  }
  private persistCaptured(): Promise<void> {
    return this.write(async () => {
      const count = this.captured.length;
      if (!count) return;
      const state = this.state;
      if (state.nextSequence + count - 1 > 2 ** 31) {
        throw new Error('This sitting has exhausted its heartbeat sequences.');
      }
      const pending = this.captured.map((beat, index) => ({ ...beat, sequence: state.nextSequence + index }));
      await this.save({ ...state, nextSequence: state.nextSequence + count, pending: [...state.pending, ...pending] });
      this.captured.splice(0, count);
    });
  }
  discardPending(): Promise<void> {
    return this.write(async () => {
      const count = this.captured.length;
      await this.save({ ...this.state, pending: [] });
      this.captured.splice(0, count);
    });
  }
  async acknowledge(sequence: number): Promise<void> {
    await this.update((state) => ({ ...state, pending: state.pending.filter((beat) => beat.sequence !== sequence) }));
  }
  flushPending(onTotals: (session: Sitting) => void): Promise<void> {
    const operation = this.heartbeats.then(async () => {
      await this.persistCaptured();
      for (;;) {
        await this.writes;
        const beat = this.state.pending[0];
        if (!beat) break;
        const result = await api.heartbeat(beat);
        await this.acknowledge(beat.sequence);
        this.latestSession = result.session;
        onTotals(result.session);
      }
      if (this.latestSession) onTotals(this.latestSession);
    });
    this.heartbeats = operation.catch(() => undefined);
    return operation;
  }
  async clear(): Promise<void> {
    await this.heartbeats;
    await this.writes;
    await setSessionValue(this.storageKey, null);
    await setSessionValue(this.ledgerKey, null);
    journals.delete(this.storageKey);
  }
}
export function openSittingJournal(course: string, identity: string): SittingJournal {
  const key = `study-session:${course}:${identity}`;
  const existing = journals.get(key);
  if (existing) return existing;
  const journal = new SittingJournal(course, identity);
  journals.set(key, journal);
  return journal;
}
