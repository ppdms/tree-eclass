import { isRouteErrorResponse, useLoaderData, useRevalidator, useRouteError, type RouteObject } from 'react-router';
import type { loadExercises } from '../app/loaders';
import { Text, View } from '../ui/primitives';
import { ExerciseAction } from './controls';
import { ExercisesPage } from './page';
import { styles } from './styles';

function ExercisesRoutePage() {
  const data = useLoaderData<typeof loadExercises>();
  return <ExercisesPage initialData={data} />;
}

function ExercisesRouteError() {
  const error = useRouteError();
  const revalidator = useRevalidator();
  const message = isRouteErrorResponse(error)
    ? error.statusText || `Server returned HTTP ${error.status}`
    : error instanceof Error
      ? error.message
      : 'Network connection issue.';
  return (
    <View id="exercises-error" ready style={styles.page}>
      <Text style={styles.title}>Exercises</Text>
      <View style={styles.message}>
        <Text style={styles.error}>Could not load exercises: {message}</Text>
        <ExerciseAction
          id="exercises-retry"
          disabled={revalidator.state === 'loading'}
          onClick={() => {
            void revalidator.revalidate();
          }}
        >
          {revalidator.state === 'loading' ? 'Opening exercises…' : 'Try again'}
        </ExerciseAction>
      </View>
    </View>
  );
}

export const exercisesRoute: RouteObject = {
  path: 'exercises',
  Component: ExercisesRoutePage,
  ErrorBoundary: ExercisesRouteError,
};
