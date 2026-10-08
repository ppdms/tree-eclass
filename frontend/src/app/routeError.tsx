import { isRouteErrorResponse, useRouteError } from 'react-router';
import { ApiError } from '@/lib/api';
import { ErrorView } from './ErrorView';
import NotFound from './not-found';

const reloadedBuilds = new Set<string>();

/** A failed lazy chunk means this tab runs an older build than the server: the
 * only recovery is a full reload, exactly once per build digest. */
function staleChunkReload(): boolean {
  const build = document.documentElement.dataset.treeBuild || 'unknown';
  if (reloadedBuilds.has(build)) return false;
  reloadedBuilds.add(build);
  window.location.reload();
  return true;
}

export function isChunkLoadError(error: Error | { status?: number } | null | undefined | unknown): boolean {
  if (!(error instanceof Error)) return false;
  const message = `${error.name}: ${error.message}`;
  return (
    message.includes('Failed to fetch dynamically imported module') ||
    message.includes('Importing a module script failed') ||
    message.includes('ChunkLoadError') ||
    message.includes('Loading chunk') ||
    message.includes('disallowed MIME type')
  );
}

export function RouteError() {
  const error = useRouteError();
  if ((error instanceof ApiError || isRouteErrorResponse(error)) && error.status === 404) return <NotFound />;
  if (isChunkLoadError(error) && staleChunkReload()) return null;
  return (
    <ErrorView
      error={{
        title: 'This page could not load',
        message: 'tree-eClass could not load this page. Your saved data has not changed.',
        recovery: 'Try again, or continue from Activity or Study.',
      }}
      reset={() => window.location.reload()}
    />
  );
}
