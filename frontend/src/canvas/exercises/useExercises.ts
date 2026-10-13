import { useEffect, useRef, useState, type Dispatch, type SetStateAction } from 'react';
import type { Exercise } from '@/lib/types';
import { requestJson } from '../app/api';
import { nextExercisesOrdinal, type ExercisesRouteData } from './freshness';
import { exerciseKey, exercisePath, type ExerciseIdentity } from './model';

export interface MutationState {
  busy: boolean;
  error: string | null;
}

interface ConfirmedIgnore {
  ordinal: number;
  ignored: boolean;
}

interface MutationContext {
  requests: Map<string, AbortController>;
  confirmed: Map<string, ConfirmedIgnore>;
  setStates: Dispatch<SetStateAction<Record<string, MutationState>>>;
  setExercises: Dispatch<SetStateAction<Exercise[]>>;
  setLastHidden: Dispatch<SetStateAction<ExerciseIdentity | null>>;
}

function reconcileRead(data: ExercisesRouteData, confirmed: Map<string, ConfirmedIgnore>): Exercise[] {
  const exercises = data.exercises.map((exercise) => {
    const change = confirmed.get(exerciseKey(exercise));
    return change && change.ordinal > data.readStarted ? { ...exercise, ignored: change.ignored } : exercise;
  });
  // A read started after confirmation is authoritative; it retires the local field overlay.
  for (const [key, change] of confirmed) {
    if (change.ordinal <= data.readStarted) confirmed.delete(key);
  }
  return exercises;
}

async function saveIgnored(context: MutationContext, exercise: ExerciseIdentity, ignored: boolean): Promise<void> {
  const key = exerciseKey(exercise);
  if (context.requests.has(key)) return;
  const controller = new AbortController();
  context.requests.set(key, controller);
  context.setStates((states) => ({ ...states, [key]: { busy: true, error: null } }));
  try {
    await requestJson<{ ok: boolean }>(`${exercisePath(exercise)}/${ignored ? 'ignore' : 'unignore'}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
      signal: controller.signal,
    });
    if (controller.signal.aborted) return;
    context.confirmed.set(key, { ordinal: nextExercisesOrdinal(), ignored });
    context.setExercises((exercises) =>
      exercises.map((item) => (exerciseKey(item) === key ? { ...item, ignored } : item)),
    );
    context.setLastHidden((previous) => {
      if (ignored) return { course_id: exercise.course_id, exercise_id: exercise.exercise_id };
      return previous && exerciseKey(previous) === key ? null : previous;
    });
    context.setStates((states) => ({ ...states, [key]: { busy: false, error: null } }));
  } catch (error) {
    if (!controller.signal.aborted) {
      const message = error instanceof Error ? error.message : 'The change could not be saved. Try again.';
      context.setStates((states) => ({ ...states, [key]: { busy: false, error: message } }));
    }
  } finally {
    if (context.requests.get(key) === controller) context.requests.delete(key);
  }
}

export function useExercises(initialData: ExercisesRouteData) {
  const [exercises, setExercises] = useState(initialData.exercises);
  const [lastHidden, setLastHidden] = useState<ExerciseIdentity | null>(null);
  const [states, setStates] = useState<Record<string, MutationState>>({});
  const requests = useRef(new Map<string, AbortController>()).current;
  const confirmed = useRef(new Map<string, ConfirmedIgnore>()).current;
  const acceptedRead = useRef(initialData.readStarted);
  useEffect(() => {
    if (initialData.readStarted <= acceptedRead.current) return;
    acceptedRead.current = initialData.readStarted;
    const incoming = reconcileRead(initialData, confirmed);
    setExercises(incoming);
    setLastHidden((previous) =>
      previous && incoming.some((exercise) => exerciseKey(exercise) === exerciseKey(previous) && exercise.ignored)
        ? previous
        : null,
    );
  }, [initialData, confirmed]);
  useEffect(
    () => () => {
      requests.forEach((controller) => controller.abort());
      requests.clear();
      confirmed.clear();
    },
    [requests, confirmed],
  );
  const save = (exercise: ExerciseIdentity, ignored: boolean) =>
    saveIgnored({ requests, confirmed, setStates, setExercises, setLastHidden }, exercise, ignored);
  return { exercises, states, lastHidden, save };
}
