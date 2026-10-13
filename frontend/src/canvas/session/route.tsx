import { useLoaderData, useRevalidator, useRouteError, type RouteObject } from 'react-router';
import type { loadSessionQuery } from '../app/loaders';
import { AppLink } from '../app/links';
import { Button, Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { message } from './api';
import { styles } from './styles';
import type { SessionQuery } from './types';
import { useSessionContext } from './useContext';
import { SessionWorkspace } from './workspace';

function SessionRoutePage() {
  const query = useLoaderData<typeof loadSessionQuery>();
  return <SessionPage key={query.identity} query={query} />;
}
function SessionPage({ query }: { query: SessionQuery }) {
  const state = useSessionContext(query);
  if (state.context) return <SessionWorkspace query={query} context={state.context} />;
  if (state.error)
    return (
      <View id="session-context-error" ready style={styles.frame}>
        <Text style={styles.heading}>Workspace unavailable</Text>
        <Notice tone="danger">{state.error}</Notice>
        <Button id="session-context-retry" onClick={state.retry}>
          Try again
        </Button>
        <AppLink href="/study" style={styles.body}>
          Back to plan
        </AppLink>
      </View>
    );
  return (
    <View id="session-context-loading" style={styles.frame}>
      <Text style={styles.heading}>
        {state.pending ? 'This document is still being prepared' : 'Opening study session…'}
      </Text>
      <Text style={styles.body}>
        {state.pending
          ? 'The registered source is indexing. This workspace checks again every five seconds.'
          : 'Loading the course, source and saved marks.'}
      </Text>
      <AppLink href="/study" style={styles.body}>
        Back to plan
      </AppLink>
    </View>
  );
}
function SessionRouteError() {
  const error = useRouteError();
  const revalidator = useRevalidator();
  return (
    <View id="session-route-error" ready style={styles.frame}>
      <Text style={styles.heading}>Workspace unavailable</Text>
      <Notice tone="danger">{message(error)}</Notice>
      <Button
        id="session-route-retry"
        disabled={revalidator.state === 'loading'}
        onClick={() => {
          void revalidator.revalidate();
        }}
      >
        Try again
      </Button>
      <AppLink href="/study" style={styles.body}>
        Back to plan
      </AppLink>
    </View>
  );
}
export const sessionRoute: RouteObject = {
  path: 'study/session',
  Component: SessionRoutePage,
  ErrorBoundary: SessionRouteError,
};
