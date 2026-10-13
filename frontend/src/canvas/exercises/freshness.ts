import type { ExercisesPayload } from '@/lib/types';

/** Client-only loader metadata; the native API response is unchanged. */
export interface ExercisesRouteData extends ExercisesPayload {
  readStarted: number;
}

let ordinal = 0;

/** Orders read starts against confirmed writes, not request completion order. */
export function nextExercisesOrdinal(): number {
  ordinal += 1;
  return ordinal;
}
