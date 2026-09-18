import { ErrorPage, type PageError } from './ErrorPage';

export function ErrorView({ error, reset }: { error: PageError; reset?: () => void }) {
  return <ErrorPage error={error} onRetry={reset} />;
}
