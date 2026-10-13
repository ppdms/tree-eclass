import type { StoryContext } from '@storybook/react-vite';
import { http, HttpResponse, passthrough } from 'msw';
import { setupWorker } from 'msw/browser';
import { initializeBrowser } from '@/shell/bootstrap';
import { setTheme } from '@/shell/navChrome';
import { DIRTY_FORMS } from '@/features/settings/state';
import { prepareApplication } from './application';
import { currentReview } from './state';
import type { ReviewSpec } from './scenario';

const worker = setupWorker();
let started = false;
let initialized = false;

const courses = [
  { id: 101, name: 'Linear Algebra' },
  { id: 202, name: 'Algorithms' },
  { id: 303, name: 'Statistics' },
];

const denyUnexpected = http.all('*', ({ request }) => {
  const url = new URL(request.url);
  if (url.origin === location.origin && !url.pathname.startsWith('/api/') && !url.pathname.startsWith('/files/'))
    return passthrough();
  currentReview().errors.push(`Blocked unhandled offline request: ${request.method} ${request.url}`);
  return HttpResponse.error();
});

async function startBoundary() {
  if (started) return;
  await worker.start({
    quiet: true,
    serviceWorker: { url: '/mockServiceWorker.js' },
    onUnhandledFrame: 'error',
  });
  started = true;
}

function resetReview(spec: ReviewSpec) {
  localStorage.clear();
  sessionStorage.clear();
  DIRTY_FORMS.clear();
  document.documentElement.dataset.treeRuntime = 'ui-review';
  document.documentElement.dataset.treeMode = 'review';
  document.documentElement.dataset.treeStorage = 'tree-eclass:ui-review:';
  window.__TREE_UI_REVIEW__ = {
    status: 'running',
    fixture: { id: spec.fixture.id, source: 'synthetic' },
    requiredCheckpoints: spec.checkpoints,
    checkpoints: [],
    expectedHttpErrors: spec.expectedHttpErrors || [],
    expectedNetworkErrors: spec.expectedNetworkErrors || [],
    downloads: spec.downloads || [],
    errors: [],
    theme: spec.theme || 'dark',
  };
}

export async function prepareScenario(context: Pick<StoryContext, 'parameters'>) {
  // SAFETY: only reviewed() supplies this parameter in the catalog's CSF stories.
  const spec = context.parameters.review as ReviewSpec;
  if (!spec) throw new Error('Every catalog story must declare a review scenario');
  resetReview(spec);
  await startBoundary();
  worker.resetHandlers(
    ...spec.fixture.handlers(),
    http.get('/api/v1/navigation/courses', () => HttpResponse.json({ courses })),
    http.get('/api/runtime/session', () => HttpResponse.json({ session: 'ui-review', mode: 'review' })),
    denyUnexpected,
  );
  if (!initialized) {
    initializeBrowser();
    initialized = true;
  }
  setTheme(spec.theme || 'dark');
  await prepareApplication(spec.path);
}
