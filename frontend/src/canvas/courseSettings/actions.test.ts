import assert from 'node:assert/strict';
import test from 'node:test';
import { ApiError } from '@/lib/api';
import { courseConfirmation, renameCourse, runCourseAction } from './actions';

const course = { id: 42, name: 'Algorithms', webdav_folder: 'algorithms' };

test('confirmation contracts keep destructive intent, destinations and preservation copy distinct', () => {
  const check = courseConfirmation(course, 'check');
  const reset = courseConfirmation(course, 'reset');
  const hide = courseConfirmation(course, 'hide');
  const deletion = courseConfirmation(course, 'delete');
  assert.equal(check.title, 'Run course check?');
  assert.equal(check.target, '/courses/42');
  assert.equal(reset.title, 'Reset course data?');
  assert.match(reset.message, /Course settings will stay/);
  assert.equal(reset.target, '/courses/42');
  assert.equal(hide.target, '/courses');
  assert.match(hide.message, /files stay synchronized/);
  assert.equal(deletion.target, '/courses');
  assert.match(deletion.message, /notes and highlights.*cannot be undone/);
});

test('rename trims the name, while scoped check retains the returned enqueue-only response', async (context) => {
  const calls: { path: string; options?: RequestInit }[] = [];
  const queued = { status: 'started', message: 'Check started in background' };
  context.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL, options?: RequestInit) => {
    calls.push({ path: String(input), options });
    return Response.json(queued);
  });
  await renameCourse(42, '  New name  ');
  const result = await runCourseAction('check', 42);
  assert.equal(calls[0]?.path, '/api/v1/courses/42');
  assert.equal(calls[0]?.options?.method, 'PATCH');
  assert.equal(calls[0]?.options?.body, JSON.stringify({ name: 'New name' }));
  assert.equal(calls[1]?.path, '/api/run-check');
  assert.equal(calls[1]?.options?.method, 'POST');
  assert.equal(calls[1]?.options?.body, JSON.stringify({ course_id: 42 }));
  assert.deepEqual(result, queued);
  assert.equal(calls.length, 2);
});

test('each destructive write carries exact confirmation and a safe key; hide has neither', async (context) => {
  const calls: { path: string; options?: RequestInit }[] = [];
  context.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL, options?: RequestInit) => {
    calls.push({ path: String(input), options });
    return Response.json({ status: 'ok', id: 42 });
  });
  await runCourseAction('reset', 42);
  await runCourseAction('delete', 42);
  await runCourseAction('hide', 42);
  for (const [index, action] of ['reset', 'delete'].entries()) {
    const call = calls[index]!;
    const headers = new Headers(call.options?.headers);
    assert.equal(call.path, `/api/v1/courses/42/${action}`);
    assert.equal(call.options?.body, '{}');
    assert.equal(headers.get('X-Tree-Eclass-Confirmation'), `${action}:42`);
    assert.match(headers.get('X-Idempotency-Key') || '', /^[A-Za-z0-9._:-]{1,128}$/);
    assert.equal(headers.get('Content-Type'), 'application/json');
  }
  const hide = new Headers(calls[2]?.options?.headers);
  assert.equal(hide.get('X-Tree-Eclass-Confirmation'), null);
  assert.equal(hide.get('X-Idempotency-Key'), null);
});

test('destructive replay and detail-array errors remain failures, without an automatic retry', async (context) => {
  let requests = 0;
  context.mock.method(globalThis, 'fetch', async () => {
    requests += 1;
    return Response.json(
      { detail: [{ msg: 'This mutation was already processed' }, 'Reconfirm your intent'] },
      {
        status: 409,
      },
    );
  });
  await assert.rejects(runCourseAction('delete', 42), (error) => {
    assert.ok(error instanceof ApiError);
    assert.equal(error.status, 409);
    assert.equal(error.message, 'This mutation was already processed. Reconfirm your intent');
    return true;
  });
  assert.equal(requests, 1);
});
