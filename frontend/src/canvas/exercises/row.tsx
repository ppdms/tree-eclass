import type { Exercise } from '@/lib/types';
import { AppLink } from '../app/links';
import { Icon, Text, View } from '../ui/primitives';
import { ExerciseDetails } from './details';
import { deadlineLabel, exerciseKey } from './model';
import { styles } from './styles';
import type { MutationState } from './useExercises';

interface ExerciseRowProps {
  exercise: Exercise;
  mutation?: MutationState;
  onSave: (ignored: boolean) => void;
}

export function ExerciseRow({ exercise, mutation, onSave }: ExerciseRowProps) {
  const submitted = exercise.submission_status === 'submitted';
  const overdue = !submitted && exercise._urgency === 'ex-overdue';
  const urgencyStyle = submitted ? styles.success : overdue ? styles.danger : styles.info;
  return (
    <View
      id={`exercise-row-${exerciseKey(exercise)}`}
      style={{ ...styles.row, ...(exercise.ignored ? styles.ignored : undefined) }}
    >
      <View style={{ ...styles.mark, ...urgencyStyle }}>
        {overdue ? <Text>!</Text> : <Icon name={submitted ? 'check' : 'clock'} style={{ width: 18, height: 18 }} />}
      </View>
      <View style={styles.main}>
        <View style={styles.heading}>
          <View style={styles.titleGroup}>
            <Text style={styles.state}>
              {exercise.ignored ? 'Hidden · ' : ''}
              {exercise.submission_status || 'Upcoming'}
            </Text>
            <AppLink href={exercise.link || '#'} target="_blank" rel="noopener" style={styles.assignment}>
              {exercise.title || 'Untitled assignment'} ↗
            </AppLink>
          </View>
          <Text style={{ ...styles.deadline, ...(submitted ? styles.quiet : undefined) }}>
            {deadlineLabel(exercise)}
          </Text>
        </View>
        <View style={styles.metadata}>
          <Text style={styles.muted}>{exercise.course_name}</Text>
          {exercise.grade && <Text style={{ ...styles.muted, ...styles.success }}>Grade {exercise.grade}</Text>}
        </View>
        <ExerciseDetails exercise={exercise} mutation={mutation} onSave={onSave} />
      </View>
    </View>
  );
}
