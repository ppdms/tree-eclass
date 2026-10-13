import assert from 'node:assert/strict';
import { configureAPI } from '../../src/canvas/app/api';

interface WireRequest {
  path: string;
  method: string;
  runtime: string | null;
  contentType: string | null;
}

interface BoundaryState {
  runtime: string;
  page: { runtime: string };
  notices: number;
  writes: number[][];
  wire: WireRequest[];
  arrived: PromiseWithResolvers<void>;
  held: PromiseWithResolvers<void>;
}

const state: BoundaryState = {
  runtime: 'development-one',
  page: { runtime: 'development-one' },
  notices: 0,
  writes: [],
  wire: [],
  arrived: Promise.withResolvers<void>(),
  held: Promise.withResolvers<void>(),
};

function record(request: Request): void {
  state.wire.push({
    path: new URL(request.url).pathname,
    method: request.method,
    runtime: request.headers.get('X-Tree-Runtime'),
    contentType: request.headers.get('Content-Type'),
  });
}

async function admit(request: Request): Promise<Response> {
  record(request);
  const path = new URL(request.url).pathname;
  if (path === '/read-conflict') return Response.json({ code: 'runtime_changed' }, { status: 409 });
  const mutation = !['GET', 'HEAD', 'OPTIONS'].includes(request.method);
  if (mutation && request.headers.get('X-Tree-Runtime') !== state.runtime)
    return Response.json({ code: 'runtime_changed' }, { status: 409 });
  if (path === '/ordinary-conflict') return Response.json({ code: 'busy' }, { status: 409 });
  if (path === '/text-conflict') return new Response('duplicate', { status: 409 });
  if (path === '/hold') {
    state.arrived.resolve();
    await state.held.promise;
    return new Response('late');
  }
  if (mutation) state.writes.push([...new Uint8Array(await request.arrayBuffer())]);
  return new Response(null, { status: 204 });
}

const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: admit });
const external = Bun.serve({
  hostname: '127.0.0.1',
  port: 0,
  fetch(request) {
    record(request);
    return Response.json({ code: 'runtime_changed' }, { status: 409 });
  },
});

async function uploadBytes(): Promise<void> {
  const request = new Request(new URL('/api/upload', server.url), {
    method: 'POST',
    body: new Uint8Array([0, 65, 255, 66]),
    headers: { 'Content-Type': 'application/octet-stream', 'X-Tree-Runtime': 'caller-cannot-upgrade' },
  });
  assert.equal((await fetch(request)).status, 204);
  assert.deepEqual(state.writes, [[0, 65, 255, 66]]);
  assert.equal(state.wire[0]!.runtime, 'development-one');
  assert.equal(state.wire[0]!.contentType, 'application/octet-stream');
  const overridden = new Request(new URL('/api/override', server.url));
  assert.equal((await fetch(overridden, { method: 'PATCH', body: 'changed method' })).status, 204);
  assert.equal(state.wire[1]!.method, 'PATCH');
  assert.equal(state.wire[1]!.runtime, 'development-one');
  assert.equal(new TextDecoder().decode(Uint8Array.from(state.writes[1]!)), 'changed method');
}

async function cancelRequests(): Promise<void> {
  const cancellation = new AbortController();
  const request = new Request(new URL('/hold', server.url), {
    method: 'POST',
    body: 'cancelled upload',
    signal: cancellation.signal,
  });
  const pending = fetch(request);
  await state.arrived.promise;
  cancellation.abort();
  await assert.rejects(pending, { name: 'AbortError' });
  state.held.resolve();
  const explicit = new AbortController();
  explicit.abort();
  const before = state.wire.length;
  await assert.rejects(
    fetch(new Request(new URL('/api/upload', server.url), { method: 'POST', body: 'not submitted' }), {
      signal: explicit.signal,
    }),
    { name: 'AbortError' },
  );
  assert.equal(state.wire.length, before, 'an explicit aborted signal must prevent submission');
}

async function rejectRefreshedAuthority(): Promise<void> {
  state.page.runtime = 'stable-two';
  state.runtime = state.page.runtime;
  const before = state.writes.length;
  const response = await fetch(new URL('/api/annotations/1', server.url), {
    method: 'DELETE',
    headers: { 'X-Tree-Runtime': state.page.runtime },
  });
  assert.equal(response.status, 409);
  assert.equal(response.bodyUsed, false);
  assert.equal(state.wire.at(-1)!.runtime, 'development-one');
  assert.equal(state.writes.length, before);
  assert.equal(state.notices, 1);
  assert.deepEqual(await response.json(), { code: 'runtime_changed' });
  assert.throws(() =>
    configureAPI(state.page.runtime, () => {
      state.notices++;
    }),
  );
  assert.equal((await fetch(new URL('/api/still-stale', server.url), { method: 'POST' })).status, 409);
  assert.equal(state.writes.length, before, 'a second configuration cannot admit later writes');
}

async function leaveReadsAndExternalRequestsAlone(): Promise<void> {
  const notices = state.notices;
  for (const method of ['GET', 'HEAD', 'OPTIONS']) {
    const response = await fetch(new URL('/read-conflict', server.url), { method });
    assert.equal(response.status, 409);
    assert.equal(state.wire.at(-1)!.runtime, null);
    assert.equal(response.bodyUsed, false);
    if (method !== 'HEAD') assert.deepEqual(await response.json(), { code: 'runtime_changed' });
  }
  const response = await fetch(new URL('/external', external.url), {
    method: 'POST',
    body: 'external bytes',
    headers: { 'X-Tree-Runtime': 'external-owned' },
  });
  assert.equal(state.wire.at(-1)!.runtime, 'external-owned');
  assert.equal(response.bodyUsed, false);
  assert.deepEqual(await response.json(), { code: 'runtime_changed' });
  assert.equal(state.notices, notices);
}

async function preserveBusinessConflicts(): Promise<void> {
  state.runtime = 'development-one';
  const notices = state.notices;
  const ordinary = await fetch(new URL('/ordinary-conflict', server.url), { method: 'PATCH' });
  assert.equal(ordinary.bodyUsed, false);
  assert.deepEqual(await ordinary.json(), { code: 'busy' });
  const text = await fetch(new URL('/text-conflict', server.url), { method: 'POST' });
  assert.equal(text.bodyUsed, false);
  assert.equal(await text.text(), 'duplicate');
  assert.equal(state.notices, notices);
}

try {
  Object.defineProperty(globalThis, 'self', {
    configurable: true,
    value: { location: { origin: server.url.origin, href: new URL('/settings', server.url).href } },
  });
  configureAPI(state.page.runtime, () => {
    state.notices++;
  });
  await uploadBytes();
  await cancelRequests();
  await rejectRefreshedAuthority();
  await leaveReadsAndExternalRequestsAlone();
  await preserveBusinessConflicts();
} finally {
  state.held.resolve();
  server.stop(true);
  external.stop(true);
}
