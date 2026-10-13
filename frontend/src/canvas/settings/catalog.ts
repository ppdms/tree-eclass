import { useCallback, useEffect, useRef, useState } from 'react';
import { z } from 'zod/v4';
import { requestJson } from '../app/api';
import { publishCatalogChanged } from '../app/events';
import { message, useSettingsContext } from './shared';
import {
  beginCatalogRead,
  confirmCatalogRegistration,
  patchCatalogRow,
  queueCatalogRows,
  reconcileCatalogRegistrations,
  reconcileCatalogRows,
  type AvailableCourse,
  type CatalogRegistrations,
  type CatalogRows,
  type CatalogRowState,
} from './catalogState';

const catalogSchema = z.object({
  courses: z
    .array(
      z.object({
        id: z.union([z.string(), z.number()]),
        code: z.string().optional().default(''),
        title: z.string().optional().default(''),
        name: z.string().optional().default(''),
        professor: z.string().optional().default(''),
        registered: z.boolean().optional().default(false),
      }),
    )
    .optional()
    .default([]),
});
export interface CatalogModel {
  courses: AvailableCourse[] | null;
  rows: Record<string, CatalogRowState>;
  loading: boolean;
  error: string | null;
  load(): Promise<void>;
  patch(id: string, update: Partial<CatalogRowState>): void;
  add(course: AvailableCourse): Promise<void>;
}
interface CourseRegistrationRequest {
  course_id: number;
  name: string;
  short_name?: string;
}

async function getCatalog(known: string[], signal: AbortSignal): Promise<AvailableCourse[]> {
  const parsed = catalogSchema.safeParse(await requestJson<unknown>('/api/v1/courses/available', { signal }));
  if (!parsed.success) throw new Error('Unexpected registered-course response.');
  return parsed.data.courses.map((course) => ({
    ...course,
    id: String(course.id),
    title: course.title || course.name,
    name: course.name || course.title,
    registered: course.registered || known.includes(String(course.id)),
  }));
}

async function registerCourse(course: AvailableCourse, name: string, shortName: string): Promise<void> {
  const body: CourseRegistrationRequest = { course_id: Number(course.id), name };
  if (shortName) body.short_name = shortName;
  await requestJson('/api/v1/courses', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

function useCatalogReads(knownKey: string) {
  const [courses, setCourses] = useState<AvailableCourse[] | null>(null);
  const [rows, setRows] = useState<CatalogRows>({});
  const rowsRef = useRef(rows);
  const updateRows = useCallback((update: (previous: CatalogRows) => CatalogRows) => {
    queueCatalogRows(rowsRef, setRows, update);
  }, []);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const controller = useRef<AbortController | null>(null);
  const registrations = useRef<CatalogRegistrations>({ sequence: 0, confirmed: new Map() });
  const load = useCallback(async () => {
    controller.current?.abort();
    const current = new AbortController();
    controller.current = current;
    const readSequence = beginCatalogRead(registrations.current);
    setLoading(true);
    setError(null);
    try {
      const result = await getCatalog(knownKey.split(',').filter(Boolean), current.signal);
      if (current.signal.aborted) return;
      setCourses(reconcileCatalogRegistrations(registrations.current, result, readSequence));
      updateRows((previous) => reconcileCatalogRows(previous, result));
    } catch (failure) {
      if (!current.signal.aborted) setError(message(failure, 'Could not load registered courses.'));
    } finally {
      if (!current.signal.aborted) setLoading(false);
    }
  }, [knownKey, updateRows]);
  useEffect(() => {
    void load();
    return () => controller.current?.abort();
  }, [load]);
  return { courses, setCourses, rows, rowsRef, updateRows, loading, error, load, registrations };
}
export function useCatalog(knownIDs: string[]): CatalogModel {
  const model = useCatalogReads(knownIDs.join(','));
  const context = useSettingsContext();
  const pending = useRef(new Set<string>());
  const patch = (id: string, update: Partial<CatalogRowState>) => {
    model.updateRows((previous) => patchCatalogRow(previous, id, update));
  };
  const add = async (course: AvailableCourse) => {
    const row = model.rowsRef.current[course.id];
    if (!row || pending.current.has(course.id) || course.registered) return;
    const name = row.name.trim(),
      shortName = row.shortName.trim();
    if (!name) {
      patch(course.id, { error: 'Course name is required.' });
      return;
    }
    pending.current.add(course.id);
    patch(course.id, { busy: true, error: null });
    try {
      await registerCourse(course, name, shortName);
      confirmCatalogRegistration(model.registrations.current, course.id);
      model.setCourses(
        (previous) =>
          previous?.map((item) => (item.id === course.id ? { ...item, registered: true, name } : item)) ?? null,
      );
      patch(course.id, { busy: false, error: null });
      context.refresh();
      publishCatalogChanged();
    } catch (failure) {
      patch(course.id, { busy: false, error: message(failure, 'Could not add course.') });
    } finally {
      pending.current.delete(course.id);
    }
  };
  return {
    courses: model.courses,
    rows: model.rows,
    loading: model.loading,
    error: model.error,
    load: model.load,
    patch,
    add,
  };
}
