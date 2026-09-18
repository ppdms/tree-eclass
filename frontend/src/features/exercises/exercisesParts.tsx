import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import { fetchJson } from '@/lib/api';
import { errorLikeSchema, errorMessage } from '@/lib/errors';
import type { Exercise, ExercisesPayload } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { ExerciseRow } from './exerciseRow';

const styles = stylex.create({
  exerciseFilters: {
    padding: '0.25rem',
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.5rem',
    alignItems: 'center',
    backgroundColor: colors.surface,
    display: 'flex',
    minWidth: 0,
  },
  exerciseFilterButtons: {
    gap: '0.25rem',
    display: 'flex',
    flexBasis: 'auto',
    flexGrow: 1,
    flexShrink: 1,
    minWidth: 0,
    overflowX: 'auto',
  },
  exerciseFilter: {
    borderColor: { default: 'transparent', ':hover': colors.border },
    borderRadius: layout.radiusSmall,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '0.25rem',
    paddingInline: '0.55rem',
    alignItems: 'center',
    cursor: 'pointer',
    display: 'inline-flex',
    fontSize: typography.sizeSm,
    fontWeight: typography.weightSemibold,
    transitionDuration: '150ms',
    transitionProperty: 'background-color, border-color, color',
    minHeight: '2.25rem',
  },
  exerciseFilterActive: {
    borderColor: colors.border,
    backgroundColor: colors.surfaceHover,
    color: colors.textPrimary,
  },
  exerciseFilterInactive: {
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceHover },
    color: { default: colors.textSecondary, ':hover': colors.textPrimary },
  },
  exerciseFilterCount: {
    fontSize: '0.68em',
    opacity: 0.75,
    marginLeft: '0.2rem',
  },
  exerciseFilterStatus: {
    flex: 'none',
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    whiteSpace: 'nowrap',
  },
  exerciseList: {
    gap: '0.35rem',
    display: 'grid',
    marginTop: '1rem',
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
  emptyText: {
    color: colors.textSecondary,
    fontSize: typography.sizeBase,
    marginBottom: '1rem',
  },
  exerciseUndoStrip: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.75rem',
    marginBlock: '0.75rem',
    paddingBlock: '0.65rem',
    paddingInline: '0.8rem',
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    color: colors.textSecondary,
    display: 'flex',
  },
  mlAuto: {
    marginLeft: 'auto',
  },
});

export type ExerciseFilter = 'all' | 'overdue' | 'upcoming' | 'completed' | 'hidden';
const EXERCISE_FILTER_LABELS = {
  all: 'All',
  overdue: 'Overdue',
  upcoming: 'Upcoming',
  completed: 'Completed',
  hidden: 'Hidden',
} satisfies Record<ExerciseFilter, string>;

// SAFETY: Object.keys over a satisfies-typed literal yields exactly the
// ExerciseFilter keys; the cast narrows the string array.
const FILTER_KEYS = Object.keys(EXERCISE_FILTER_LABELS) as ExerciseFilter[];

export interface ExerciseCounts {
  all: number;
  overdue: number;
  upcoming: number;
  completed: number;
  hidden: number;
}

export interface LastHidden {
  courseId: string | number;
  exerciseId: string | number;
}

export function exerciseCounts(exercises: Exercise[]): ExerciseCounts {
  return {
    all: exercises.filter((item) => !item.ignored).length,
    overdue: exercises.filter((item) => item._urgency === 'ex-overdue' && !item.ignored).length,
    upcoming: exercises.filter(
      (item) => item.submission_status !== 'submitted' && item._urgency !== 'ex-overdue' && !item.ignored,
    ).length,
    completed: exercises.filter((item) => item.submission_status === 'submitted' && !item.ignored).length,
    hidden: exercises.filter((item) => item.ignored).length,
  };
}

export function ExerciseFilters({
  filter,
  setFilter,
  count,
  counts,
}: {
  filter: ExerciseFilter;
  setFilter: (filter: ExerciseFilter) => void;
  count: number;
  counts: ExerciseCounts;
}) {
  return (
    <nav {...stylex.props(styles.exerciseFilters)} aria-label="Exercise filters">
      <ExerciseFilterButtons filter={filter} counts={counts} onChange={setFilter} />
      <span {...stylex.props(styles.exerciseFilterStatus)}>
        {count} assignment{count === 1 ? '' : 's'}
      </span>
    </nav>
  );
}

function ExerciseFilterButtons({
  filter,
  counts,
  onChange,
}: {
  filter: ExerciseFilter;
  counts: ExerciseCounts;
  onChange: (filter: ExerciseFilter) => void;
}) {
  return (
    <div {...stylex.props(styles.exerciseFilterButtons)}>
      {FILTER_KEYS.filter((value) => value === 'all' || counts[value] > 0).map((value) => (
        <button
          key={value}
          type="button"
          {...stylex.props(
            styles.exerciseFilter,
            filter === value ? styles.exerciseFilterActive : styles.exerciseFilterInactive,
          )}
          aria-pressed={filter === value}
          onClick={() => onChange(value)}
        >
          {EXERCISE_FILTER_LABELS[value]} <span {...stylex.props(styles.exerciseFilterCount)}>{counts[value]}</span>
        </button>
      ))}
    </div>
  );
}

export function filterExercises(exercises: Exercise[], filter: ExerciseFilter): Exercise[] {
  if (filter === 'completed')
    return exercises.filter((exercise) => exercise.submission_status === 'submitted' && !exercise.ignored);
  if (filter === 'overdue')
    return exercises.filter((exercise) => exercise._urgency === 'ex-overdue' && !exercise.ignored);
  if (filter === 'upcoming')
    return exercises.filter(
      (exercise) =>
        exercise.submission_status !== 'submitted' && exercise._urgency !== 'ex-overdue' && !exercise.ignored,
    );
  if (filter === 'hidden') return exercises.filter((exercise) => exercise.ignored);
  return exercises.filter((exercise) => !exercise.ignored);
}

export interface UseExercisesResult {
  data: ExercisesPayload | null;
  loading: boolean;
  error: string | null;
  lastHidden: LastHidden | null;
  hideExercise: (courseId: string | number, exerciseId: string | number) => void;
  restoreExercise: (courseId: string | number, exerciseId: string | number) => void;
}

export function useExercises(initialData: ExercisesPayload | null): UseExercisesResult {
  const [data, setData] = React.useState<ExercisesPayload | null>(initialData);
  const [loading, setLoading] = React.useState(!initialData);
  const [error, setError] = React.useState<string | null>(null);
  const [lastHidden, setLastHidden] = React.useState<LastHidden | null>(null);
  React.useEffect(() => {
    if (initialData) return; // Route data is already in state; standalone mounts fetch
    fetchJson<ExercisesPayload>('api/v1/exercises?include_ignored=true&include_details=false')
      .then(setData)
      .catch((err) => {
        const parsed = errorLikeSchema.safeParse(err);
        const errorLike = parsed.success ? parsed.data : { message: String(err) };
        setError(errorMessage(errorLike, 'Network connection issue'));
      })
      .finally(() => setLoading(false));
  }, [initialData]);
  const updateExercise = (courseId: string | number, exerciseId: string | number, patch: Partial<Exercise>) =>
    setData((current) => ({
      ...current,
      exercises: (current?.exercises || []).map((exercise) =>
        exercise.course_id === courseId && exercise.exercise_id === exerciseId ? { ...exercise, ...patch } : exercise,
      ),
    }));
  const hideExercise = (courseId: string | number, exerciseId: string | number) => {
    setLastHidden({ courseId, exerciseId });
    updateExercise(courseId, exerciseId, { ignored: true });
  };
  const restoreExercise = (courseId: string | number, exerciseId: string | number) => {
    updateExercise(courseId, exerciseId, { ignored: false });
    setLastHidden(null);
  };
  return { data, loading, error, lastHidden, hideExercise, restoreExercise };
}

export function ExerciseList({
  exercises,
  counts,
  filter,
  setFilter,
  lastHidden,
  hideExercise,
  restoreExercise,
}: {
  exercises: Exercise[];
  counts: ExerciseCounts;
  filter: ExerciseFilter;
  setFilter: (filter: ExerciseFilter) => void;
  lastHidden: LastHidden | null;
  hideExercise: (courseId: string | number, exerciseId: string | number) => void;
  restoreExercise: (courseId: string | number, exerciseId: string | number) => void;
}) {
  return (
    <>
      <ExerciseFilters filter={filter} setFilter={setFilter} count={exercises.length} counts={counts} />
      {lastHidden && <UndoStrip lastHidden={lastHidden} onRestore={restoreExercise} />}
      {!exercises.length ? (
        <ExerciseEmptyState filter={filter} setFilter={setFilter} />
      ) : (
        <div {...stylex.props(styles.exerciseList)}>
          {exercises.map((exercise, index) => (
            <ExerciseRow
              key={exercise.exercise_id || index}
              exercise={exercise}
              onIgnore={hideExercise}
              onRestore={restoreExercise}
            />
          ))}
        </div>
      )}
    </>
  );
}

function ExerciseEmptyState({
  filter,
  setFilter,
}: {
  filter: ExerciseFilter;
  setFilter: (filter: ExerciseFilter) => void;
}) {
  return (
    <div {...stylex.props(styles.exerciseEmpty)}>
      <p {...stylex.props(styles.emptyText)}>
        {filter === 'hidden'
          ? 'No hidden exercises. Hidden assignments appear here so they can be restored.'
          : 'No exercises match this filter.'}
      </p>
      {filter !== 'all' && (
        <button
          type="button"
          {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
          onClick={() => setFilter('all')}
        >
          Show all exercises
        </button>
      )}
    </div>
  );
}

export function UndoStrip({
  lastHidden,
  onRestore,
}: {
  lastHidden: LastHidden;
  onRestore: (courseId: string | number, exerciseId: string | number) => void;
}) {
  return (
    <div {...stylex.props(styles.exerciseUndoStrip)} role="status">
      Assignment hidden.{' '}
      <button
        type="button"
        {...stylex.props(buttonStyles.base, buttonStyles.secondary, styles.mlAuto)}
        onClick={() => onRestore(lastHidden.courseId, lastHidden.exerciseId)}
      >
        Undo
      </button>
    </div>
  );
}
