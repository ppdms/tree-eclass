export interface AvailableCourse {
  id: string;
  title: string;
  name: string;
  code: string;
  professor: string;
  registered: boolean;
}
export interface CatalogRowState {
  name: string;
  shortName: string;
  busy: boolean;
  error: string | null;
}
export type CatalogRows = Record<string, CatalogRowState>;
type RowUpdate = (previous: CatalogRows) => CatalogRows;

export function reconcileCatalogRows(previous: CatalogRows, courses: AvailableCourse[]): CatalogRows {
  return Object.fromEntries(
    courses.map((course) => [
      course.id,
      previous[course.id] ?? { name: course.name, shortName: '', busy: false, error: null },
    ]),
  );
}

export function patchCatalogRow(previous: CatalogRows, id: string, update: Partial<CatalogRowState>): CatalogRows {
  const kept = previous[id];
  return kept ? { ...previous, [id]: { ...kept, ...update } } : previous;
}

/** Project every queued transform into the submission ref, without replacing React's queued state. */
export function queueCatalogRows(
  current: { current: CatalogRows },
  enqueue: (update: RowUpdate) => void,
  update: RowUpdate,
): void {
  current.current = update(current.current);
  enqueue(update);
}

export interface CatalogRegistrations {
  sequence: number;
  confirmed: Map<string, number>;
}
export function beginCatalogRead(registrations: CatalogRegistrations): number {
  return ++registrations.sequence;
}
export function confirmCatalogRegistration(registrations: CatalogRegistrations, id: string): void {
  registrations.confirmed.set(id, ++registrations.sequence);
}

/** Only reads started before a committed Add need a local registration overlay. */
export function reconcileCatalogRegistrations(
  registrations: CatalogRegistrations,
  courses: AvailableCourse[],
  readSequence: number,
): AvailableCourse[] {
  const result = courses.map((course) => ({
    ...course,
    registered: course.registered || (registrations.confirmed.get(course.id) ?? 0) > readSequence,
  }));
  for (const [id, sequence] of registrations.confirmed) {
    if (sequence < readSequence) registrations.confirmed.delete(id);
  }
  return result;
}
