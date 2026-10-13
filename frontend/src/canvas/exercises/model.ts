import type { Exercise } from '@/lib/types';

export type ExerciseFilter = 'all' | 'overdue' | 'upcoming' | 'completed' | 'hidden';
export type ExerciseCounts = Record<ExerciseFilter, number>;
export type ExerciseIdentity = Pick<Exercise, 'course_id' | 'exercise_id'>;

export const filters: { id: ExerciseFilter; label: string }[] = [
  { id: 'all', label: 'All' },
  { id: 'overdue', label: 'Overdue' },
  { id: 'upcoming', label: 'Upcoming' },
  { id: 'completed', label: 'Completed' },
  { id: 'hidden', label: 'Hidden' },
];

export const exercisesPath = '/api/v1/exercises?include_ignored=true&include_details=false';

export function exerciseKey(exercise: ExerciseIdentity): string {
  return JSON.stringify([exercise.course_id, exercise.exercise_id]);
}

export function exercisePath(exercise: ExerciseIdentity): string {
  const course = encodeURIComponent(String(exercise.course_id));
  const id = encodeURIComponent(String(exercise.exercise_id));
  return `/api/v1/courses/${course}/exercises/${id}`;
}

export function matchesFilter(exercise: Exercise, filter: ExerciseFilter): boolean {
  if (filter === 'hidden') return Boolean(exercise.ignored);
  if (exercise.ignored) return false;
  if (filter === 'completed') return exercise.submission_status === 'submitted';
  if (filter === 'overdue') return exercise._urgency === 'ex-overdue';
  if (filter === 'upcoming') {
    return exercise.submission_status !== 'submitted' && exercise._urgency !== 'ex-overdue';
  }
  return true;
}

export function exerciseCounts(exercises: Exercise[]) {
  const counts = { all: 0, overdue: 0, upcoming: 0, completed: 0, hidden: 0 } satisfies ExerciseCounts;
  for (const exercise of exercises) {
    for (const filter of filters) {
      if (matchesFilter(exercise, filter.id)) counts[filter.id]++;
    }
  }
  return counts;
}

export function deadlineLabel(exercise: Exercise): string {
  if (exercise._time_label) return exercise._time_label;
  const short = String(exercise.deadline_short || '').replace(' · ', ', ');
  if (short) return exercise.submission_status === 'submitted' ? `Due ${short}` : short;
  return exercise.deadline || 'No deadline';
}
