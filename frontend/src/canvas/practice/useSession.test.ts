import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { act, createElement, useState, type Dispatch, type SetStateAction } from 'react';
import { z } from 'zod/v4';
import { mount, type CanvasContainer } from '../ui/react.ts';
import { usePracticeSession, type PracticeSession } from './useSession.ts';
import { usePracticeView, type PracticeViewResource } from './useView.ts';
import type { CoursePracticeView, PracticeAttemptBody } from './model.ts';

const VIEW: CoursePracticeView = {
  course_id: 101,
  enabled: true,
  blueprint_revision_id: 'same-revision',
  units: [
    {
      unit_key: 'one',
      title: 'Unit one',
      questions: ['first', 'second'].map((id) => ({
        question_id: id,
        unit_key: 'one',
        blueprint_revision_id: 'same-revision',
        set_hash: 'same-set',
        prompt: id,
        expected_answer: `${id} answer`,
      })),
    },
  ],
};

const attemptSchema = z
  .object({
    course_id: z.union([z.string(), z.number()]),
    question_id: z.string(),
    outcome: z.enum(['correct', 'partial', 'incorrect', 'skipped']),
    idempotency_key: z.string(),
    seconds: z.number().optional(),
    note: z.string().optional(),
  })
  .passthrough() satisfies z.ZodType<PracticeAttemptBody>;

function nullHostContainer(): CanvasContainer {
  const children: CanvasContainer['root']['children'] = [];
  const root = new Proxy(
    { children },
    {
      get(target, key) {
        if (key === 'children') return target.children;
        throw new Error(`The hook-only harness cannot access root.${String(key)}`);
      },
    },
  );
  // SAFETY: this reconciler test renders null, so only root.children and commit
  // callbacks are reachable. Both proxies fail closed on every host/graphics port.
  return new Proxy(
    { root, beginCommit() {}, endCommit() {} },
    {
      get(target, key) {
        if (key === 'root') return target.root;
        if (key === 'beginCommit') return target.beginCommit;
        if (key === 'endCommit') return target.endCommit;
        throw new Error(`The hook-only harness cannot access ${String(key)}`);
      },
    },
  ) as CanvasContainer;
}

class Harness {
  session!: PracticeSession;
  resource!: PracticeViewResource;
  setVersion!: Dispatch<SetStateAction<number>>;
  close?: Dispatch<SetStateAction<boolean>>;
  readResponse: Promise<Response> | null = null;
  attemptResponse: Response | Promise<Response> = Response.json(
    { detail: 'Temporary publication conflict' },
    { status: 409 },
  );
  attempts: PracticeAttemptBody[] = [];
  reads = 0;
}

function deferred() {
  let resolve!: (response: Response) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<Response>((accept, decline) => {
    resolve = accept;
    reject = decline;
  });
  return { promise, resolve, reject };
}

function environment(t: TestContext, state: Harness): void {
  const previous = ['self', 'IS_REACT_ACT_ENVIRONMENT'].map(
    (key) => [key, Object.getOwnPropertyDescriptor(globalThis, key)] as const,
  );
  Object.assign(globalThis, { self: globalThis, IS_REACT_ACT_ENVIRONMENT: true });
  t.after(async () => {
    try {
      if (state.close)
        await act(async () => {
          state.close!(false);
        });
    } finally {
      for (const [key, descriptor] of previous) {
        if (descriptor) Object.defineProperty(globalThis, key, descriptor);
        else Reflect.deleteProperty(globalThis, key);
      }
    }
  });
}

async function createHarness(t: TestContext): Promise<Harness> {
  const state = new Harness();
  environment(t, state);
  t.mock.method(globalThis, 'fetch', (path: RequestInfo | URL, options?: RequestInit) => {
    if (options?.method === 'POST') {
      assert.equal(path, '/api/study/practice/attempt');
      state.attempts.push(attemptSchema.parse(JSON.parse(String(options.body))));
      return Promise.resolve(state.attemptResponse).then((response) => response.clone());
    }
    assert.equal(path, '/api/study/practice?course_id=101');
    state.reads++;
    return state.readResponse || Promise.resolve(Response.json(VIEW));
  });
  function Consumer() {
    const [version, update] = useState(0);
    state.setVersion = update;
    state.resource = usePracticeView(101, version);
    state.session = usePracticeSession(
      101,
      state.resource.view,
      state.resource.setView,
      'same-revision',
      state.resource.generation,
      state.resource.current,
    );
    return null;
  }
  function Root() {
    const [open, update] = useState(true);
    state.close = update;
    return open ? createElement(Consumer) : null;
  }
  // Exercise the production reconciler/hooks without a graphics surface.
  const tree = nullHostContainer();
  await act(async () => {
    mount(tree, createElement(Root), (error) => {
      throw error;
    });
  });
  return state;
}

async function blockFailedAndInvalidRefreshes(state: Harness): Promise<void> {
  const pending = deferred();
  state.readResponse = pending.promise;
  await act(async () => {
    state.setVersion(1);
  });
  assert.equal(state.session.conflict, true, 'request initiation must not admit the old queue');
  assert.equal(state.session.note, 'original draft');
  await act(async () => {
    pending.reject(new Error('Refresh unavailable'));
  });
  assert.equal(state.resource.generation, 1);
  assert.equal(state.session.conflict, true);
  await act(async () => {
    await state.session.record('correct');
  });
  assert.equal(state.attempts.length, 1);
  state.readResponse = Promise.resolve(Response.json({ ...VIEW, enabled: false }));
  await act(async () => {
    state.setVersion(2);
  });
  assert.equal(state.session.conflict, true, 'disabled practice is not a fresh admission');
  await act(async () => {
    await state.session.record('correct');
  });
  assert.equal(state.attempts.length, 1);
  state.readResponse = Promise.resolve(Response.json({ ...VIEW, blueprint_revision_id: 'other-revision' }));
  await act(async () => {
    state.setVersion(3);
  });
  assert.equal(state.session.conflict, true, 'a mismatched source must stay blocked');
  assert.equal(state.session.stale, true);
  await act(async () => {
    await state.session.record('correct');
  });
  assert.equal(state.attempts.length, 1);
}

async function admitMatchingRefresh(state: Harness, reordered: CoursePracticeView): Promise<void> {
  const pending = deferred();
  state.readResponse = pending.promise;
  await act(async () => {
    state.setVersion(4);
  });
  assert.equal(state.session.conflict, true);
  await act(async () => {
    pending.resolve(Response.json(reordered));
  });
  assert.equal(state.session.conflict, false);
  assert.equal(state.session.stale, false);
  assert.equal(state.session.index, 1, 'selection follows question identity, not queue position');
  assert.equal(state.session.revealed, true);
  assert.equal(state.session.note, 'original draft');
}

async function preserveOrdinaryRetry(state: Harness, reordered: CoursePracticeView): Promise<void> {
  state.attemptResponse = Response.json({ detail: 'Ordinary request failure' }, { status: 500 });
  await act(async () => {
    await state.session.record('correct');
  });
  assert.notEqual(state.attempts[1]!.idempotency_key, state.attempts[0]!.idempotency_key);
  assert.equal(state.session.hasRetry, true);
  await act(async () => {
    state.session.setNote('new visible draft');
  });
  state.readResponse = Promise.resolve(Response.json(reordered));
  await act(async () => {
    state.setVersion(5);
  });
  assert.equal(state.session.note, 'new visible draft', 'unrelated progress must not reset drafts');
  assert.equal(state.session.revealed, true);
  assert.equal(state.session.hasRetry, true);
  state.attemptResponse = Response.json({
    status: 'recorded',
    attempt: { unit_key: 'one', outcome: 'correct' },
    practice: reordered,
  });
  await act(async () => {
    await state.session.record('correct');
  });
  assert.deepEqual(state.attempts[2], state.attempts[1], 'ordinary retries keep their note and request ID');
  assert.equal(state.session.hasRetry, false);
  assert.equal(state.session.revealed, true);
  assert.equal(state.session.note, 'new visible draft', 'an old retry cannot consume a newer draft');
}

async function advanceWithoutWriting(state: Harness): Promise<void> {
  const question = state.session.queue[state.session.index]!.question_id;
  const posts = state.attempts.length;
  await act(async () => {
    state.session.setRevealed(true);
    state.session.setNote('Next must discard this');
  });
  await act(async () => {
    state.session.next();
  });
  assert.notEqual(state.session.queue[state.session.index]!.question_id, question);
  assert.equal(state.session.revealed, false);
  assert.equal(state.session.note, '');
  assert.equal(state.attempts.length, posts, 'Next is local navigation, never a skipped attempt');
}

async function replaceSetDespiteObsoleteRead(state: Harness, reordered: CoursePracticeView): Promise<void> {
  await act(async () => {
    state.session.setRevealed(true);
    state.session.setNote('old question-set draft');
  });
  const previousGeneration = state.resource.generation;
  const pending = deferred();
  state.readResponse = pending.promise;
  await act(async () => {
    state.setVersion(6);
  });
  const replaced = {
    ...VIEW,
    units: [
      {
        ...VIEW.units![0],
        questions: VIEW.units![0]!.questions!.map((question) => ({
          ...question,
          set_hash: 'replacement-set',
        })),
      },
    ],
  };
  state.readResponse = Promise.resolve(Response.json(replaced));
  await act(async () => {
    state.setVersion(7);
  });
  await act(async () => {
    pending.resolve(Response.json(reordered));
  });
  assert.equal(state.resource.generation, previousGeneration + 1, 'an aborted old read is never accepted');
  assert.equal(state.session.queue[state.session.index]!.set_hash, 'replacement-set');
  assert.equal(state.session.revealed, false, 'a changed source cannot reuse an old revealed answer');
  assert.equal(state.session.note, '');
}

test('practice admits matching reads, preserves retries and Next, and rejects obsolete source state', async (t) => {
  const state = await createHarness(t);
  assert.equal(state.reads, 1);
  await act(async () => {
    state.session.setRevealed(true);
    state.session.setNote('original draft');
  });
  await act(async () => {
    await state.session.record('correct');
  });
  assert.equal(state.session.conflict, true);
  assert.equal(state.attempts.length, 1);
  await blockFailedAndInvalidRefreshes(state);
  const reordered = { ...VIEW, units: [{ ...VIEW.units![0], questions: [...VIEW.units![0]!.questions!].reverse() }] };
  await admitMatchingRefresh(state, reordered);
  await preserveOrdinaryRetry(state, reordered);
  await advanceWithoutWriting(state);
  await replaceSetDespiteObsoleteRead(state, reordered);
});

test('a late attempt acknowledgement preserves the next question and its active draft', async (t) => {
  const state = await createHarness(t);
  const pending = deferred();
  state.attemptResponse = pending.promise;
  await act(async () => {
    state.session.setRevealed(true);
    state.session.setNote('submitted note');
  });
  let saving!: Promise<void>;
  await act(async () => {
    saving = state.session.record('correct');
  });
  await act(async () => {
    state.session.next();
  });
  const active = state.session.queue[state.session.index]!.question_id;
  await act(async () => {
    state.session.setRevealed(true);
    state.session.setNote('new active note');
  });
  await act(async () => {
    pending.resolve(Response.json({ attempt: { unit_key: 'one', outcome: 'correct' }, practice: VIEW }));
    await saving;
  });
  assert.equal(state.attempts[0]!.question_id, 'first');
  assert.equal(state.attempts[0]!.note, 'submitted note');
  assert.equal(active, 'second');
  assert.equal(state.session.queue[state.session.index]!.question_id, active);
  assert.equal(state.session.note, 'new active note');
  assert.equal(state.session.revealed, true);
  assert.equal(state.session.busy, false);
  assert.equal(state.session.hasRetry, false);
});
