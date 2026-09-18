import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import type { ExercisesPayload } from '@/lib/types';
import { commonStyles } from '@/styles/common';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import {
  exerciseCounts,
  filterExercises,
  useExercises,
  ExerciseList,
  type ExerciseFilter,
  type ExerciseCounts,
} from './exercisesParts';

const styles = stylex.create({
  exerciseInbox: {
    marginInline: 'auto',
    marginBottom: '3rem',
    maxWidth: '73.75rem',
    minWidth: 0,
    width: '100%',
  },
  exerciseIntro: {
    marginBottom: '0.75rem',
  },
  exerciseIntroText: {
    fontFamily: typography.fontFamily,
    fontSize: typography.sizeBase,
    lineHeight: layout.bodyLeading,
  },
  exerciseEmpty: {
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'dashed',
    borderWidth: 1,
    paddingBlock: '3rem',
    paddingInline: '1rem',
    backgroundColor: colors.surface,
    textAlign: 'center',
  },
  errorActions: {
    marginTop: '0.75rem',
  },
});

export interface ExercisesPageProps {
  initialData?: ExercisesPayload | null;
}

export default function ExercisesPage({ initialData = null }: ExercisesPageProps) {
  const { data, loading, error, lastHidden, hideExercise, restoreExercise } = useExercises(initialData);
  const [filter, setFilter] = React.useState<ExerciseFilter>('all');
  const allExercises = data?.exercises || [];
  const counts = exerciseCounts(allExercises);
  const exercises = filterExercises(allExercises, filter);
  return (
    <ExercisesPageContent
      {...{
        loading,
        error,
        exercises,
        counts,
        filter,
        setFilter,
        lastHidden,
        hideExercise,
        restoreExercise,
      }}
    />
  );
}

function ExercisesPageContent({
  loading,
  error,
  exercises,
  counts,
  filter,
  setFilter,
  lastHidden,
  hideExercise,
  restoreExercise,
}: {
  loading: boolean;
  error: string | null;
  exercises: ExercisesPayload['exercises'];
  counts: ExerciseCounts;
  filter: ExerciseFilter;
  setFilter: (filter: ExerciseFilter) => void;
  lastHidden: { courseId: string | number; exerciseId: string | number } | null;
  hideExercise: (courseId: string | number, exerciseId: string | number) => void;
  restoreExercise: (courseId: string | number, exerciseId: string | number) => void;
}) {
  return (
    <div {...stylex.props(styles.exerciseInbox)}>
      <header {...stylex.props(styles.exerciseIntro)}>
        <h1 {...stylex.props(commonStyles.srOnly)}>Exercises</h1>
        <p {...stylex.props(styles.exerciseIntroText)}>Assignments, details, grades, and deadlines in one place.</p>
      </header>
      {loading ? (
        <ExerciseMessage role="status">Opening exercises…</ExerciseMessage>
      ) : error ? (
        <ExerciseError error={error} />
      ) : (
        <ExerciseList {...{ exercises, counts, filter, setFilter, lastHidden, hideExercise, restoreExercise }} />
      )}
    </div>
  );
}

function ExerciseMessage({ role, children }: { role: 'status' | 'alert'; children: React.ReactNode }) {
  return (
    <div {...stylex.props(styles.exerciseEmpty)} role={role}>
      {children}
    </div>
  );
}

function ExerciseError({ error }: { error: string }) {
  return (
    <ExerciseMessage role="alert">
      Could not load exercises: {error}
      <div {...stylex.props(styles.errorActions)}>
        <button
          type="button"
          {...stylex.props(buttonStyles.base, buttonStyles.primary)}
          onClick={() => window.location.reload()}
        >
          Try again
        </button>
      </div>
    </ExerciseMessage>
  );
}
