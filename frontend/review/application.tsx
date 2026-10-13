import { createMemoryRouter } from 'react-router';
import { RouterProvider } from 'react-router/dom';
import { configureNavigation } from './navigation';

let active: ReturnType<typeof createMemoryRouter> | undefined;
let unsubscribe: (() => void) | undefined;
let generation = 0;

function syncQuery(path: string) {
  const application = new URL(path, location.origin);
  const preview = new URL(location.href);
  preview.hash = application.hash;
  const query = new URLSearchParams(application.search);
  for (const key of ['id', 'viewMode', 'globals']) {
    const value = preview.searchParams.get(key);
    if (value !== null) query.set(key, value);
  }
  preview.search = query.toString();
  history.replaceState(history.state, '', preview);
}

export async function prepareApplication(path: string) {
  generation++;
  unsubscribe?.();
  active?.dispose();
  // Reuse the authoritative route table without activating its browser-history
  // owner in the catalog. The preview adapter keeps Storybook's iframe URL intact.
  const { routes } = await import('@/app/routes');
  syncQuery(path);
  active = createMemoryRouter(routes, { initialEntries: [path] });
  unsubscribe = active.subscribe((state) =>
    syncQuery(state.location.pathname + state.location.search + state.location.hash),
  );
  configureNavigation(active, routes);
}

export function applicationGeneration() {
  return generation;
}

export function ReviewApplication() {
  if (!active) throw new Error('Scenario application was not prepared');
  return <RouterProvider router={active} />;
}
