import assert from 'node:assert/strict';
import test from 'node:test';
import { configurePreferences, sessionValue } from '../app/preferences.ts';
import { openSittingJournal, SittingJournal } from './journal.ts';
import { codePointLength } from './api.ts';

const beat = {
  session_id: 501,
  document_id: 'source_Ελληνικά\u0000',
  source_hash: 'synthetic-source-001',
  page_number: 7,
  interval_seconds: 15,
  active: true,
};

test('a reusable sitting key is not visible until the native storage write acknowledges', async () => {
  let release!: () => void;
  const acknowledged = new Promise<void>((resolve) => {
    release = resolve;
  });
  const writes: string[] = [];
  configurePreferences({ local: {}, session: {} }, async (scope, key) => {
    writes.push(`${scope}:${key}`);
    await acknowledged;
  });
  const journal = new SittingJournal('101', 'doc:source_Ελληνικά');
  const saving = journal.prepare();
  assert.equal(sessionValue(journal.storageKey), null);
  assert.equal(writes.length, 1);
  release();
  await saving;
  assert.equal(sessionValue(journal.storageKey), journal.state.key);
  assert.equal(writes.length, 2);
});

test('legacy client keys adopt the authoritative server sequence instead of restarting at zero', async () => {
  configurePreferences(
    {
      local: {},
      session: {
        'study-session:101:legacy-action': 'sess_legacy_request_key',
      },
    },
    async () => undefined,
  );
  const journal = new SittingJournal('101', 'legacy-action');
  await journal.prepare();
  await journal.bind(501, 12);
  await journal.enqueue(beat);
  assert.equal(journal.state.key, 'sess_legacy_request_key');
  assert.equal(journal.state.pending[0]?.sequence, 12);
  assert.equal(journal.state.nextSequence, 13);
});

test('durable exact pending beats survive reload and retain their sequence below the server watermark', async () => {
  configurePreferences({ local: {}, session: {} }, async () => undefined);
  const journal = new SittingJournal('101', 'semantic-action');
  await journal.prepare();
  await journal.bind(501, 0);
  await Promise.all([journal.enqueue(beat), journal.enqueue({ ...beat, page_number: 8, active: false })]);
  const restored = new SittingJournal('101', 'semantic-action');
  await restored.bind(501, 5);
  assert.deepEqual(restored.state.pending, [
    { ...beat, sequence: 0 },
    { ...beat, page_number: 8, active: false, sequence: 1 },
  ]);
  assert.equal(restored.state.nextSequence, 5);
  await restored.acknowledge(0);
  assert.equal(restored.state.pending[0]?.sequence, 1);
  await restored.enqueue({ ...beat, page_number: 9 });
  assert.equal(restored.state.pending[1]?.sequence, 5);
});

test('rejected native persistence neither advances the sequence nor pretends the beat is durable', async () => {
  const saved = { key: 'sess_storage_failure', sessionId: 501, nextSequence: 9, pending: [] };
  configurePreferences(
    {
      local: {},
      session: {
        'study-session:101:write-failure': saved.key,
        'study-session-ledger:101:write-failure': JSON.stringify(saved),
      },
    },
    async () => {
      throw new Error('Synthetic native storage rejection');
    },
  );
  const journal = new SittingJournal('101', 'write-failure');
  await assert.rejects(journal.enqueue(beat), /Synthetic native storage rejection/);
  assert.equal(journal.state.nextSequence, 9);
  assert.deepEqual(journal.state.pending, []);
  assert.deepEqual(JSON.parse(sessionValue(journal.ledgerKey)!), saved);
});

test('retry preserves captured attention after storage rejection instead of draining an empty journal', async (t) => {
  let unavailable = false;
  configurePreferences({ local: {}, session: {} }, async () => {
    if (unavailable) throw new Error('Synthetic storage rejection');
  });
  const journal = new SittingJournal('101', 'captured-before-storage-failure');
  await journal.prepare();
  await journal.bind(501, 9);
  unavailable = true;
  await assert.rejects(journal.enqueue(beat), /Synthetic storage rejection/);
  unavailable = false;
  const sent: unknown[] = [];
  const totals: number[] = [];
  t.mock.method(globalThis, 'fetch', async (_url: RequestInfo | URL, init: RequestInit) => {
    sent.push(JSON.parse(String(init.body)));
    return Response.json({ status: 'recorded', session: { id: 501, active_seconds: 15, visible_seconds: 15 } });
  });
  await journal.flushPending((session) => totals.push(session.active_seconds));
  assert.deepEqual(sent, [{ ...beat, sequence: 9 }]);
  assert.equal(totals.at(-1), 15);
  assert.deepEqual(journal.state.pending, []);
  assert.equal(journal.state.nextSequence, 10);
});

test('a same-worker remount shares the journal while old and new page records await acknowledgment', async () => {
  configurePreferences({ local: {}, session: {} }, async () => undefined);
  const previous = openSittingJournal('101', 'same-worker-remount');
  await previous.prepare();
  await previous.bind(501, 0);
  const oldPage = previous.enqueue(beat);
  const restored = openSittingJournal('101', 'same-worker-remount');
  const newPage = restored.enqueue({ ...beat, page_number: 8 });
  assert.equal(restored, previous);
  await Promise.all([oldPage, newPage]);
  assert.deepEqual(
    restored.state.pending.map((item) => [item.sequence, item.page_number]),
    [
      [0, 7],
      [1, 8],
    ],
  );
  await restored.clear();
});

test('learner text bounds count Greek, NUL, private-use, astral and unpaired Unicode code points', () => {
  assert.equal(codePointLength('Ε\u0000\ue000😀'), 4);
  assert.equal(codePointLength('\ud800x\udc00'), 3);
  assert.equal(codePointLength('😀'.repeat(4000)), 4000);
});
