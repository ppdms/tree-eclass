import { useLoaderData, useParams } from 'react-router';
import type { RouteObject } from 'react-router';
import type { CourseDetailPayload } from '@/lib/types';
import { RouteError } from '../app/shell';
import { Text, View } from '../ui/primitives';
import { CoursePage } from './page';
import { styles } from './styles';

function CourseRoute() {
  const { courseId } = useParams();
  const data = useLoaderData<CourseDetailPayload>();
  return <CoursePage key={courseId} data={data} />;
}

function CourseLoading() {
  return (
    <View id="course-loading" style={styles.page}>
      <Text style={styles.description}>Opening course…</Text>
    </View>
  );
}

export const courseDetailRoute: RouteObject = {
  path: 'courses/:courseId',
  Component: CourseRoute,
  ErrorBoundary: RouteError,
  HydrateFallback: CourseLoading,
};
