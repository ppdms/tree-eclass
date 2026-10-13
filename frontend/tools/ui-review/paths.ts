import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import budgets from '../../performance-budgets.json';
import { z } from 'zod/v4';

export const frontend = fileURLToPath(new URL('../../', import.meta.url));
export const output = resolve(frontend, '.ui-review');
export const catalog = resolve(output, 'catalog');
export const runs = resolve(output, 'runs');
export const viewport = { width: 1440, height: 1000 };
export const environment = {
  locale: 'en-GB',
  timezoneId: 'Europe/Athens',
  colorScheme: 'dark' as const,
  deviceScaleFactor: 1,
};
export const fixedTime = '2026-10-13T10:00:00.000Z';
export const concurrency = z.number().int().min(1).max(4).parse(budgets.uiReviewConcurrentContexts);
export const scenarioTimeout = z.number().int().min(1000).max(120000).parse(budgets.uiReviewScenarioTimeoutMs);
