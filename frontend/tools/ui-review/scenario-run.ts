import { mkdir, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import type { Browser, BrowserContext, Page } from '@playwright/test';
import { environment, viewport, fixedTime, scenarioTimeout } from './paths';
import { stateSchema } from './schemas';
import { digest, saveJson } from './provenance';
import type { ScenarioResult } from './evidence';
import { Monitor } from './monitor';
import { Downloads } from './downloads';

async function screenshot(page: Page, result: ScenarioResult, root: string, name: string) {
  if (!/^[a-z0-9-]+$/.test(name) || result.screenshots.some((item) => item.name === name)) {
    throw new Error(`Invalid or duplicate screenshot checkpoint: ${name}`);
  }
  if (name !== 'failure') await page.evaluate(() => document.fonts.ready);
  const path = `${result.scenario.id}/${name}.png`;
  await page.screenshot({
    path: resolve(root, path),
    animations: 'disabled',
    caret: 'hide',
    timeout: name === 'failure' ? 2000 : scenarioTimeout,
  });
  result.screenshots.push({ name, path, sha256: digest(await readFile(resolve(root, path))) });
}

async function reviewState(page: Page) {
  return stateSchema.parse(await page.evaluate(() => window.__TREE_UI_REVIEW__));
}

function assertCheckpoints(result: ScenarioResult) {
  const state = result.review;
  if (!state) return;
  const required = state.requiredCheckpoints;
  if (!required.length || new Set(required).size !== required.length) {
    result.errors.push('Scenario must declare unique, nonempty required checkpoints');
  }
  for (const name of required) {
    if (!state.checkpoints.includes(name) || !result.screenshots.some((item) => item.name === name)) {
      result.errors.push(`Missing required screenshot: ${name}`);
    }
  }
}

async function exercise(page: Page, result: ScenarioResult, url: string, root: string, downloads: Downloads) {
  await page.exposeFunction('__treeCaptureCheckpoint', async (name: string) => {
    const state = await reviewState(page);
    if (!state.requiredCheckpoints.includes(name)) throw new Error(`Undeclared checkpoint: ${name}`);
    await downloads.checkpoint(name, state);
    await screenshot(page, result, root, name);
  });
  await page.clock.setFixedTime(new Date(fixedTime));
  await page.goto(`${url}/iframe.html?id=${encodeURIComponent(result.scenario.id)}&viewMode=story`, {
    waitUntil: 'load',
    timeout: scenarioTimeout,
  });
  await page.waitForFunction(
    () => window.__TREE_UI_REVIEW__?.status !== undefined && window.__TREE_UI_REVIEW__.status !== 'running',
    undefined,
    { timeout: scenarioTimeout },
  );
  result.review = await reviewState(page);
  result.errors.push(...result.review.errors);
  if (result.review.status !== 'passed') result.errors.push('Story interaction did not pass');
  assertCheckpoints(result);
}

async function finishContext(context: BrowserContext, result: ScenarioResult, root: string, monitor: Monitor) {
  try {
    const trace = `${result.scenario.id}/trace.zip`;
    await context.tracing.stop({ path: resolve(root, trace) });
    result.trace = trace;
  } catch (error) {
    result.errors.push(`Trace retention: ${error instanceof Error ? error.message : String(error)}`);
  }
  try {
    const path = `${result.scenario.id}/browser.json`;
    await saveJson(resolve(root, path), monitor.evidence());
    result.browserLog = path;
  } finally {
    await context.close();
  }
}

async function interruptScenario(page: Page, result: ScenarioResult, root: string, downloads: Downloads) {
  downloads.cancel('Capture interrupted');
  await screenshot(page, result, root, 'failure').catch((error: Error) => result.errors.push(error.message));
  await page.close().catch(() => {});
}

export async function runScenario(
  browser: Browser,
  result: ScenarioResult,
  url: string,
  root: string,
  signal: AbortSignal,
) {
  await mkdir(resolve(root, result.scenario.id), { recursive: true });
  result.status = 'running';
  result.startedAt = new Date().toISOString();
  const context = await browser.newContext({ ...environment, viewport, acceptDownloads: true });
  const monitor = new Monitor();
  const page = await context.newPage();
  const downloads = new Downloads(root, result);
  let stopping: Promise<void> | undefined;
  const abort = () => {
    stopping ??= interruptScenario(page, result, root, downloads);
  };
  signal.addEventListener('abort', abort, { once: true });
  if (signal.aborted) abort();
  try {
    await monitor.attach(context, url);
    downloads.attach(page);
    await context.tracing.start({ screenshots: true, snapshots: true, sources: true });
    await exercise(page, result, url, root, downloads);
  } catch (error) {
    result.errors.push(error instanceof Error ? error.message : String(error));
    if (!page.isClosed()) result.review = await reviewState(page).catch(() => undefined);
  } finally {
    signal.removeEventListener('abort', abort);
    await stopping;
    downloads.cancel('Scenario ended before download checkpoint completed');
    await downloads.finish(result.review);
    result.errors.push(...monitor.validate(result.review));
    if (result.errors.length && !page.isClosed()) {
      await screenshot(page, result, root, 'failure').catch((error: Error) => result.errors.push(error.message));
    }
    await finishContext(context, result, root, monitor);
    result.status = result.errors.length ? 'failed' : 'passed';
    result.finishedAt = new Date().toISOString();
  }
}
