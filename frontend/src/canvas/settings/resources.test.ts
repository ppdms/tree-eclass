import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { watchResource } from './resources';
import { runningCheckStatus, type CheckStatus } from './checkState';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((accept, fail) => {
    resolve = accept;
    reject = fail;
  });
  return { promise, resolve, reject };
}
const settle = () => new Promise<void>((resolve) => setImmediate(resolve));

class CheckWatcher {
  epoch = 0;
  status: CheckStatus = { is_checking: false, last_check_result: 'success' };
  readonly published: CheckStatus[] = [];
  readonly errors: (string | null)[] = [];
  readonly stops: (() => void)[] = [];

  constructor(t: TestContext) {
    t.after(() => {
      for (const stop of this.stops) stop();
    });
  }

  watch() {
    const expected = this.epoch;
    const stop = watchResource<CheckStatus>({
      path: '/api/check-status',
      interval: 60000,
      enabled: true,
      abortHidden: true,
      isCurrent: () => this.epoch === expected,
      publish: (value) => {
        this.status = value;
        this.published.push(value);
      },
      fail: (error) => {
        this.errors.push(error);
      },
    });
    this.stops.push(stop);
    return stop;
  }
}

test('check acknowledgement survives stale and failed reads until fresh status succeeds', async (t) => {
  const responses = [deferred<Response>(), deferred<Response>(), deferred<Response>()];
  let requestIndex = 0;
  t.mock.method(globalThis, 'fetch', () => responses[requestIndex++]!.promise);
  const watcher = new CheckWatcher(t);
  const old = watcher.watch();
  watcher.epoch++;
  watcher.status = runningCheckStatus(watcher.status, {});
  // The old effect has not been cleaned up yet; it must already be invalidated.
  responses[0]!.resolve(Response.json({ is_checking: false, last_check_result: 'success' }));
  await settle();
  assert.equal(watcher.status.is_checking, true);
  assert.equal(watcher.published.length, 0);
  assert.equal(watcher.errors.length, 0);
  old();

  const failed = watcher.watch();
  assert.equal(watcher.status.is_checking, true);
  responses[1]!.reject(new Error('Connection unavailable'));
  await settle();
  assert.equal(watcher.status.is_checking, true);
  assert.deepEqual(watcher.errors, ['Could not refresh course-check status.']);
  failed();

  watcher.epoch++;
  watcher.watch();
  const completed = { is_checking: false, last_check_at: '2026-10-12T08:00:00Z', last_check_result: 'success' };
  responses[2]!.resolve(Response.json(completed));
  await settle();
  assert.deepEqual(watcher.status, completed);
  assert.deepEqual(watcher.published, [completed]);
  assert.deepEqual(watcher.errors, ['Could not refresh course-check status.', null]);
});

test('an invalidated failed read cannot replace diagnostics from the current status read', async (t) => {
  const response = deferred<Response>();
  t.mock.method(globalThis, 'fetch', () => response.promise);
  let current = true;
  const errors: (string | null)[] = [];
  const stop = watchResource<CheckStatus>({
    path: '/api/check-status',
    interval: 60000,
    enabled: true,
    abortHidden: true,
    isCurrent: () => current,
    publish: () => assert.fail('An invalidated read must not publish'),
    fail: (error) => {
      errors.push(error);
    },
  });
  t.after(stop);
  current = false;
  response.reject(new Error('Old read failed'));
  await settle();
  assert.deepEqual(errors, []);
});
