import { ErrorView } from './ErrorView';

export default function NotFound() {
  return (
    <ErrorView
      error={{
        title: 'Page not found',
        message: 'The page you are looking for does not exist or has moved.',
        recovery: 'Return to Activity or open Study to continue.',
      }}
    />
  );
}
