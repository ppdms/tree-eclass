import { isRouteErrorResponse, useRevalidator, useRouteError } from 'react-router';
import { ApiError } from '@/lib/api';
import { ErrorView } from './ErrorView';
import NotFound from './not-found';

export function RouteError() {
  const error = useRouteError();
  const revalidator = useRevalidator();
  if ((error instanceof ApiError || isRouteErrorResponse(error)) && error.status === 404) return <NotFound />;
  return (
    <ErrorView
      error={{
        title: 'This page could not load',
        message: 'tree-eClass could not load this page. Your saved data has not changed.',
        recovery: 'Try again, or continue from Activity or Study.',
      }}
      reset={() => void revalidator.revalidate()}
    />
  );
}
