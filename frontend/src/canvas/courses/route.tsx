import { isRouteErrorResponse, useLoaderData, useRevalidator, useRouteError, type RouteObject } from 'react-router';
import { Text, View } from '../ui/primitives';
import type { CoursesCatalogData } from './orderState';
import { CoursesPage } from './page';
import { styles } from './styles';

function CoursesRoute() {
  const data = useLoaderData<CoursesCatalogData>();
  return <CoursesPage data={data} />;
}

function CoursesError() {
  const error = useRouteError();
  const revalidator = useRevalidator();
  const retrying = revalidator.state === 'loading';
  const message =
    error instanceof Error
      ? error.message
      : isRouteErrorResponse(error)
        ? error.statusText
        : 'The course catalog could not load. Please try again.';
  return (
    <View id="courses-error" ready style={styles.page}>
      <View style={styles.state}>
        <Text style={styles.stateTitle}>Courses are unavailable</Text>
        <Text style={styles.description}>{message}</Text>
        <View
          id="courses-retry"
          style={styles.action}
          onClick={
            retrying
              ? undefined
              : () => {
                  void revalidator.revalidate();
                }
          }
        >
          <Text>{retrying ? 'Opening courses…' : 'Try again'}</Text>
        </View>
      </View>
    </View>
  );
}

function CoursesLoading() {
  return (
    <View id="courses-loading" style={styles.page}>
      <Text style={styles.description}>Opening courses…</Text>
    </View>
  );
}

export const coursesRoute: RouteObject = {
  path: 'courses',
  Component: CoursesRoute,
  ErrorBoundary: CoursesError,
  HydrateFallback: CoursesLoading,
};
