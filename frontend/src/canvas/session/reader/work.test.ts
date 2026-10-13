import assert from 'node:assert/strict';
import test from 'node:test';
import { settledPageWork } from './work.ts';

test('raster rejection does not release the page while its delayed text/operator work is active', async () => {
  const geometry = Promise.withResolvers<string>();
  const cancellation = new DOMException('Evicted page', 'AbortError');
  let released = false;
  const load = settledPageWork([Promise.reject(cancellation), geometry.promise] as const);
  const rejection = assert.rejects(load, (error) => error === cancellation);
  void load.then(
    () => {
      released = true;
    },
    () => {
      released = true;
    },
  );
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(released, false);
  geometry.resolve('completed operator list');
  await rejection;
  assert.equal(released, true);
});

test('geometry rejection waits for an in-flight raster before relinquishing its slot', async () => {
  const raster = Promise.withResolvers<string>();
  const failure = new Error('Geometry failure');
  let released = false;
  const load = settledPageWork([raster.promise, Promise.reject(failure)] as const);
  const rejection = assert.rejects(load, (error) => error === failure);
  void load.then(
    () => {
      released = true;
    },
    () => {
      released = true;
    },
  );
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(released, false);
  raster.resolve('discarded raster');
  await rejection;
  assert.equal(released, true);
});

test('one failed text phase cannot hide a delayed operator phase from the page scheduler', async () => {
  const operators = Promise.withResolvers<number[]>();
  const failure = new Error('Text extraction failure');
  let settled = false;
  const geometry = settledPageWork([Promise.reject(failure), operators.promise, Promise.resolve('size')] as const);
  const rejection = assert.rejects(geometry, (error) => error === failure);
  void geometry.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(settled, false);
  operators.resolve([1, 2, 3]);
  await rejection;
  assert.equal(settled, true);
});

test('complete page work preserves result order and exact values', async () => {
  const raster = { identity: 'bitmap' };
  const geometry = { text: 'Ε\u0000😀' };
  const results = await settledPageWork([Promise.resolve(raster), Promise.resolve(geometry)] as const);
  assert.equal(results[0], raster);
  assert.equal(results[1], geometry);
});
