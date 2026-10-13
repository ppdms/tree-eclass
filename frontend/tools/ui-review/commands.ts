import { spawn } from 'node:child_process';
import { copyFile, mkdir, rm, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { frontend, output } from './paths';
import { learnerExport } from '../../review/fixtures/settings-export';

const require = createRequire(import.meta.url);

export async function prepareWorker() {
  const dir = resolve(output, 'worker');
  await mkdir(dir, { recursive: true });
  await copyFile(require.resolve('msw/mockServiceWorker.js'), resolve(dir, 'mockServiceWorker.js'));
  await mkdir(resolve(dir, 'api/settings'), { recursive: true });
  await writeFile(resolve(dir, 'api/settings/export'), JSON.stringify(learnerExport, null, 2));
}

export async function catalogLock() {
  await mkdir(output, { recursive: true });
  const lock = resolve(output, 'catalog.lock');
  try {
    await mkdir(lock);
  } catch (error) {
    throw new Error('Catalog is in use by another build/capture. If interrupted, remove .ui-review/catalog.lock.', {
      cause: error,
    });
  }
  return () => rm(lock, { recursive: true, force: true });
}

export async function storybook(args: string[]) {
  const child = spawn(process.execPath, [resolve(frontend, 'node_modules/.bin/storybook'), ...args], {
    cwd: frontend,
    stdio: 'inherit',
    detached: true,
    env: { ...process.env, STORYBOOK_DISABLE_TELEMETRY: '1', DO_NOT_TRACK: '1' },
  });
  const stop = () => {
    if (child.pid && child.exitCode === null && child.signalCode === null) {
      process.kill(-child.pid, 'SIGTERM');
    }
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  try {
    const code = await new Promise<number>((accept, reject) => {
      child.once('error', reject);
      child.once('exit', (code, signal) => accept(code ?? (signal ? 130 : 1)));
    });
    if (code) throw new Error(`Storybook exited with status ${code}`);
  } finally {
    process.off('SIGINT', stop);
    process.off('SIGTERM', stop);
    stop();
  }
}
