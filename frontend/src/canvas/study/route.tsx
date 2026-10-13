import {
  isRouteErrorResponse,
  useLoaderData,
  useLocation,
  useRevalidator,
  useRouteError,
  type RouteObject,
} from 'react-router';
import type { loadStudy } from '../app/loaders';
import { reloadPage } from '../app/browser';
import { AppLink } from '../app/links';
import { Button, Notice, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { StudyPage } from './page';
import { styles } from './styles';

function StudyRoutePage() {
  const data = useLoaderData<typeof loadStudy>();
  const location = useLocation();
  return <StudyPage key={location.search} initialData={data} />;
}

function hasFailureStatus(error: ReturnType<typeof useRouteError>): error is { status: unknown } {
  return error !== null && typeof error === 'object' && 'status' in error;
}

export function StudyLoading() {
  return (
    <View style={styles.page}>
      <Notice>
        <Text>Opening study planner…</Text>
      </Notice>
    </View>
  );
}

function StudyRouteError() {
  const error = useRouteError();
  const revalidator = useRevalidator();
  const status = isRouteErrorResponse(error) ? error.status : hasFailureStatus(error) ? error.status : undefined;
  const message =
    error instanceof Error
      ? error.message
      : isRouteErrorResponse(error)
        ? error.statusText
        : 'Network connection issue';
  return (
    <View ready style={styles.page}>
      <Panel>
        <View style={styles.stack}>
          <Text style={styles.heading}>{status === 404 ? 'Page not found' : 'Could not load this study plan'}</Text>
          <Text style={styles.copy}>
            {status === 404 ? 'This study course is missing or no longer available.' : message}
          </Text>
          <Text style={styles.muted}>Your saved data has not changed.</Text>
          <View style={styles.actions}>
            <Button
              id="study-route-retry"
              disabled={revalidator.state === 'loading'}
              onClick={() => {
                void revalidator.revalidate();
              }}
            >
              {revalidator.state === 'loading' ? 'Opening study planner…' : 'Try again'}
            </Button>
            <Button id="study-route-reload" onClick={reloadPage}>
              Reload page
            </Button>
            <AppLink id="study-route-all" href="/study" style={styles.link}>
              Open all courses
            </AppLink>
            <AppLink id="study-route-activity" href="/activity" style={styles.link}>
              Activity
            </AppLink>
          </View>
        </View>
      </Panel>
    </View>
  );
}

export const studyRoute: RouteObject = {
  path: 'study',
  Component: StudyRoutePage,
  ErrorBoundary: StudyRouteError,
  HydrateFallback: StudyLoading,
};
