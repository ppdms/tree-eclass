import { isRouteErrorResponse, useLoaderData, useParams, useRouteError, type RouteObject } from 'react-router';
import { ApiError } from '@/lib/api';
import type { loadChange } from '../app/loaders';
import { AppLink } from '../app/links';
import { Text, View } from '../ui/primitives';
import { ChangeDetail } from './details';
import { changeScope } from './model';
import { styles } from './styles';

function ChangeRoutePage() {
  const data = useLoaderData<typeof loadChange>();
  return (
    <ChangeDetail
      key={changeScope(data.courseId, data.changeNo)}
      courseId={data.courseId}
      changeNo={data.changeNo}
      data={data.detail}
    />
  );
}

function ChangeRouteError() {
  const error = useRouteError();
  const { courseId = '', changeNo = '' } = useParams();
  const scope = changeScope(courseId, changeNo);
  const status = isRouteErrorResponse(error) ? error.status : error instanceof ApiError ? error.status : null;
  return (
    <View id={`change-error-${scope}`} ready style={styles.page}>
      <Text style={styles.title}>{status === 404 ? 'Page not found' : 'Could not load this change.'}</Text>
      {status === 404 && <Text style={styles.muted}>This change is missing or no longer visible.</Text>}
      <AppLink
        id={`change-error-return-${scope}`}
        href={`/courses/${encodeURIComponent(courseId)}`}
        style={styles.action}
      >
        Return to the course
      </AppLink>
    </View>
  );
}

function ChangeLoading() {
  return <Text style={{ ...styles.muted, padding: 24 }}>Opening change details…</Text>;
}

export const changeRoute: RouteObject = {
  path: 'courses/:courseId/changes/:changeNo',
  Component: ChangeRoutePage,
  ErrorBoundary: ChangeRouteError,
  HydrateFallback: ChangeLoading,
};
