import { isRouteErrorResponse, useLoaderData, useRouteError, type RouteObject } from 'react-router';
import { reloadPage } from '../app/browser';
import { AppLink } from '../app/links';
import { Button, Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import type { SettingsLoad } from './data';
import { SettingsPage } from './page';
import { styles } from './shared';

function SettingsRoute() {
  const initial = useLoaderData<SettingsLoad>();
  return <SettingsPage initial={initial} />;
}
function hasHttpStatus(cause: unknown): cause is { status: number } {
  return cause !== null && typeof cause === 'object' && 'status' in cause && typeof cause.status === 'number';
}
function SettingsError() {
  const error = useRouteError();
  const status = isRouteErrorResponse(error) ? error.status : hasHttpStatus(error) ? error.status : undefined;
  const message =
    error instanceof Error
      ? error.message
      : isRouteErrorResponse(error)
        ? error.statusText
        : 'Network connection issue';
  return (
    <View id="settings-error" ready style={styles.page}>
      <Text style={styles.title}>{status === 404 ? 'Page not found' : 'This page could not load'}</Text>
      <Notice tone="danger">{`Could not load settings (${message}).`}</Notice>
      <Button id="settings-load-retry" onClick={reloadPage}>
        Try again
      </Button>
      <AppLink id="settings-error-activity" href="/">
        Return to Activity
      </AppLink>
      <AppLink id="settings-error-study" href="/study">
        Open Study
      </AppLink>
    </View>
  );
}
function SettingsLoading() {
  return (
    <View id="settings-loading" style={styles.page}>
      <Text style={styles.muted}>Opening settings…</Text>
    </View>
  );
}
export const settingsRoute: RouteObject = {
  path: 'settings',
  Component: SettingsRoute,
  ErrorBoundary: SettingsError,
  HydrateFallback: SettingsLoading,
};
