import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { test } from 'bun:test';

test('the worker fetch bridge preserves uploads and cancellation without refreshing page authority', async () => {
  // configureAPI is intentionally immutable. Give it a process, not a reset hook
  // or a shared global fetch mutation that can interfere with another test file.
  const fixture = fileURLToPath(new URL('./runtime-guard.fixture.ts', import.meta.url));
  const child = Bun.spawn([process.execPath, fixture], {
    stdout: 'pipe',
    stderr: 'pipe',
    // Process deadline only; assertions await real HTTP and cancellation events.
    timeout: 15_000,
  });
  try {
    const [code, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    assert.equal(code, 0, stdout + stderr);
  } finally {
    if (child.exitCode === null) child.kill();
  }
}, 20_000);
