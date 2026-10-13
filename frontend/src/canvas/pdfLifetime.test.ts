import assert from 'node:assert/strict';
import test from 'node:test';
import { PdfWorkLifetime } from './pdfLifetime.ts';

test('unmount retains a pending operator job until document termination acknowledges', async () => {
  const lifetime = new PdfWorkLifetime();
  const operators = Promise.withResolvers<string>();
  const termination = Promise.withResolvers<void>();
  let settled = false;
  const work = lifetime.run(operators.promise);
  const rejection = assert.rejects(work, { name: 'AbortError' });
  void work.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  lifetime.close(termination.promise);
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(settled, false);
  termination.resolve();
  await rejection;
  assert.equal(settled, true);
});

test('an early render cancellation rejection cannot release a closing source before teardown', async () => {
  const lifetime = new PdfWorkLifetime();
  const raster = Promise.withResolvers<string>();
  const termination = Promise.withResolvers<void>();
  let settled = false;
  const work = lifetime.run(raster.promise);
  const rejection = assert.rejects(work, { name: 'AbortError' });
  void work.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  lifetime.close(termination.promise);
  raster.reject(new Error('RenderTask rejected before Terminate'));
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(settled, false);
  termination.resolve();
  await rejection;
  assert.equal(settled, true);
});

test('late successful work from a closing source is discarded only after termination', async () => {
  const lifetime = new PdfWorkLifetime();
  const text = Promise.withResolvers<string>();
  const termination = Promise.withResolvers<void>();
  let settled = false;
  const work = lifetime.run(text.promise);
  const rejection = assert.rejects(work, { name: 'AbortError' });
  void work.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  lifetime.close(termination.promise);
  text.resolve('text from the previous source');
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(settled, false);
  termination.resolve();
  await rejection;
  assert.equal(settled, true);
});

test('normal document work retains exact results and failures', async () => {
  const lifetime = new PdfWorkLifetime();
  const text = 'Ε\u0000\ue000😀';
  assert.equal(await lifetime.run(Promise.resolve(text)), text);
  const failure = new Error('PDF parse failed');
  await assert.rejects(lifetime.run(Promise.reject(failure)), (error) => error === failure);
});
