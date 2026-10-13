import { mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { chromium, type Browser } from '@playwright/test';
import { catalog, runs, viewport, environment, fixedTime, concurrency } from './paths';
import { catalogLock } from './commands';
import { requireFreshBuild, saveJson } from './provenance';
import { options, select } from './options';
import { serve } from './server';
import { runScenario } from './scenario-run';
import { gallery } from './gallery';
import type { RunManifest } from './evidence';

async function execute(manifest: RunManifest, root: string, signal: AbortSignal) {
  const server = await serve(catalog);
  let browser: Browser | undefined;
  let completed = false;
  try {
    browser = await chromium.launch({
      executablePath: process.env.UI_REVIEW_BROWSER || undefined,
      handleSIGINT: false,
      handleSIGTERM: false,
      handleSIGHUP: false,
    });
    manifest.browser = browser.version();
    let next = 0;
    const worker = async () => {
      while (!signal.aborted && next < manifest.scenarios.length) {
        const result = manifest.scenarios[next++]!;
        result.status = 'running';
        await saveJson(resolve(root, 'manifest.json'), manifest);
        await runScenario(browser!, result, server.url, root, signal);
        console.log(`${result.status.toUpperCase()} ${result.scenario.id}`);
        await saveJson(resolve(root, 'manifest.json'), manifest);
      }
    };
    const outcomes = await Promise.allSettled(Array.from({ length: concurrency }, worker));
    for (const outcome of outcomes) {
      if (outcome.status === 'rejected') {
        manifest.errors.push(outcome.reason instanceof Error ? outcome.reason.message : String(outcome.reason));
      }
    }
    completed =
      outcomes.every((outcome) => outcome.status === 'fulfilled') &&
      manifest.scenarios.every((result) => ['passed', 'failed'].includes(result.status));
  } finally {
    try {
      await browser?.close();
    } finally {
      await server.close();
    }
  }
  if (completed && !signal.aborted) manifest.status = 'completed';
}

async function reserve(name: string) {
  await mkdir(runs, { recursive: true });
  const root = resolve(runs, name);
  try {
    await mkdir(root);
  } catch (error) {
    throw new Error(`Run "${name}" already exists or cannot be created. Choose a new --out name.`, { cause: error });
  }
  return root;
}

async function capture() {
  const args = options(process.argv.slice(2));
  const unlock = await catalogLock();
  try {
    const { provenance, scenarios } = await requireFreshBuild();
    const selected = select(scenarios, args.only);
    if (args.list) {
      for (const entry of selected) console.log(`${entry.id}\t${entry.title} / ${entry.name}`);
      return;
    }
    const name = args.out || `review-${new Date().toISOString().replace(/[:.]/g, '-')}-${process.pid}`;
    const root = await reserve(name);
    const manifest: RunManifest = {
      schemaVersion: 1,
      name,
      status: 'incomplete',
      startedAt: new Date().toISOString(),
      provenance,
      viewport,
      environment,
      fixedTime,
      concurrency,
      errors: [],
      scenarios: selected.map((scenario) => ({
        scenario,
        status: 'pending',
        screenshots: [],
        downloads: [],
        errors: [],
      })),
    };
    const interrupted = await runCapture(manifest, root);
    if (manifest.status === 'incomplete') {
      console.error(`Incomplete capture: ${manifest.errors.join('\n') || 'Not every selected scenario finished'}`);
    }
    console.log(`Gallery: ${resolve(root, 'index.html')}`);
    process.exitCode =
      manifest.status !== 'completed'
        ? interrupted
          ? 130
          : 2
        : manifest.scenarios.some((result) => result.status === 'failed')
          ? 1
          : 0;
  } finally {
    await unlock();
  }
}

async function runCapture(manifest: RunManifest, root: string) {
  const controller = new AbortController();
  const interrupt = () => controller.abort();
  const signals = ['SIGINT', 'SIGTERM', 'SIGHUP'] as const;
  signals.forEach((signal) => process.on(signal, interrupt));
  try {
    await saveJson(resolve(root, 'manifest.json'), manifest);
    await execute(manifest, root, controller.signal);
  } catch (error) {
    manifest.errors.push(error instanceof Error ? error.message : String(error));
  } finally {
    signals.forEach((signal) => process.off(signal, interrupt));
    if (controller.signal.aborted) manifest.errors.push('Interrupted; selected scenarios may not have run');
    manifest.finishedAt = new Date().toISOString();
    await saveJson(resolve(root, 'manifest.json'), manifest);
    await gallery(root, manifest);
  }
  return controller.signal.aborted;
}

await capture().catch((error: Error) => {
  console.error(error.message);
  process.exitCode = 1;
});
