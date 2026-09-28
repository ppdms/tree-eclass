import { expect, test } from 'bun:test';
import { runInNewContext } from 'node:vm';
import { RUNTIME_GUARD_SCRIPT } from '../../src/shell/runtimeGuard';

function fixture() {
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
  const window = Object.assign(new EventTarget(), {
    location: { origin: 'http://localhost:8000', href: 'http://localhost:8000/settings' },
    fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input, init });
      return Response.json({ code: 'runtime_changed' }, { status: 409 });
    },
  });
  const dataset = { treeRuntime: 'development-one' };
  const document = { documentElement: { dataset }, addEventListener() {} };
  runInNewContext(RUNTIME_GUARD_SCRIPT, { window, document, Request, URL, Headers, Event });
  return { calls, window, dataset };
}

test('page generation follows SDK Request uploads without buffering or changing cancellation', async () => {
  const { calls, window, dataset } = fixture();
  const abort = new AbortController();
  const request = new Request('http://localhost:8000/api/upload', {
    method: 'POST',
    body: 'synthetic bytes',
    signal: abort.signal,
    headers: { 'Content-Type': 'text/plain' },
  });
  const response = await window.fetch(request);
  expect(response.status).toBe(409);
  expect(calls[0].input).toBe(request);
  expect(request.bodyUsed).toBe(false);
  expect(new Headers(calls[0].init?.headers).get('Content-Type')).toBe('text/plain');
  expect(new Headers(calls[0].init?.headers).get('X-Tree-Runtime')).toBe('development-one');
  dataset.treeRuntime = 'stable-two';
  await window.fetch('/api/v1/settings/credentials', { method: 'POST', signal: abort.signal });
  expect(new Headers(calls[1].init?.headers).get('X-Tree-Runtime')).toBe('development-one');
  expect(calls[1].init?.signal).toBe(abort.signal);
});

test('reads stay unchanged and stale writes notify without consuming responses', async () => {
  const { calls, window } = fixture();
  await window.fetch('/api/status');
  await window.fetch('https://example.invalid/upload', { method: 'POST' });
  expect(calls[0].init).toBeUndefined();
  expect(new Headers(calls[1].init?.headers).has('X-Tree-Runtime')).toBe(false);
  const notice = new Promise<void>((resolve) => window.addEventListener('tree-runtime-changed', () => resolve()));
  const response = await window.fetch('/api/annotations/1', { method: 'DELETE' });
  await notice;
  expect(await response.json()).toEqual({ code: 'runtime_changed' });
});
