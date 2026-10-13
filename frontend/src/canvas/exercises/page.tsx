import { useState } from 'react';
import { Text, View } from '../ui/primitives';
import { ExerciseAction } from './controls';
import type { ExercisesRouteData } from './freshness';
import {
  exerciseCounts,
  exerciseKey,
  filters,
  matchesFilter,
  type ExerciseCounts,
  type ExerciseFilter,
  type ExerciseIdentity,
} from './model';
import { ExerciseRow } from './row';
import { styles } from './styles';
import { useExercises, type MutationState } from './useExercises';

interface FilterProps {
  current: ExerciseFilter;
  counts: ExerciseCounts;
  count: number;
  onChange: (filter: ExerciseFilter) => void;
}

function ExerciseFilters({ current, counts, count, onChange }: FilterProps) {
  const visible = filters.filter((filter) => filter.id === 'all' || counts[filter.id] > 0 || filter.id === current);
  return (
    <View id="exercise-filters" style={styles.filters}>
      <View style={styles.tabs}>
        {visible.map((filter) => (
          <ExerciseAction
            key={filter.id}
            id={`exercise-filter-${filter.id}`}
            style={current === filter.id ? styles.active : undefined}
            onClick={() => onChange(filter.id)}
          >
            {filter.label} {counts[filter.id]}
          </ExerciseAction>
        ))}
      </View>
      <Text style={styles.muted}>
        {count} assignment{count === 1 ? '' : 's'}
      </Text>
    </View>
  );
}

function UndoStrip({
  identity,
  mutation,
  onRestore,
}: {
  identity: ExerciseIdentity;
  mutation?: MutationState;
  onRestore: () => void;
}) {
  return (
    <View id="exercise-undo" style={styles.undo}>
      <Text style={styles.body}>Assignment hidden.</Text>
      <ExerciseAction id={`exercise-undo-${exerciseKey(identity)}`} disabled={mutation?.busy} onClick={onRestore}>
        {mutation?.busy ? 'Restoring…' : 'Undo'}
      </ExerciseAction>
      {mutation?.error && <Text style={styles.error}>Could not undo: {mutation.error} Select Undo to try again.</Text>}
    </View>
  );
}

function EmptyExercises({ filter, onShowAll }: { filter: ExerciseFilter; onShowAll: () => void }) {
  return (
    <View id="exercises-empty" style={styles.message}>
      <Text style={styles.body}>
        {filter === 'hidden'
          ? 'No hidden exercises. Hidden assignments appear here so they can be restored.'
          : 'No exercises match this filter.'}
      </Text>
      {filter !== 'all' && (
        <ExerciseAction id="exercise-show-all" onClick={onShowAll}>
          Show all exercises
        </ExerciseAction>
      )}
    </View>
  );
}

export function ExercisesPage({ initialData }: { initialData: ExercisesRouteData }) {
  const { exercises, states, lastHidden, save } = useExercises(initialData);
  const [filter, setFilter] = useState<ExerciseFilter>('all');
  const counts = exerciseCounts(exercises);
  const visible = exercises.filter((exercise) => matchesFilter(exercise, filter));
  return (
    <View id="exercises-content" ready style={styles.page}>
      <View style={styles.intro}>
        <Text style={styles.title}>Exercises</Text>
        <Text style={styles.body}>Assignments, details, grades, and deadlines in one place.</Text>
      </View>
      <ExerciseFilters current={filter} counts={counts} count={visible.length} onChange={setFilter} />
      {lastHidden && (
        <UndoStrip
          identity={lastHidden}
          mutation={states[exerciseKey(lastHidden)]}
          onRestore={() => {
            void save(lastHidden, false);
          }}
        />
      )}
      {visible.length ? (
        <View style={styles.list}>
          {visible.map((exercise) => (
            <ExerciseRow
              key={exerciseKey(exercise)}
              exercise={exercise}
              mutation={states[exerciseKey(exercise)]}
              onSave={(ignored) => {
                void save(exercise, ignored);
              }}
            />
          ))}
        </View>
      ) : (
        <EmptyExercises filter={filter} onShowAll={() => setFilter('all')} />
      )}
    </View>
  );
}
