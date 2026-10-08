import { useRevalidator } from 'react-router';
import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { z } from 'zod/v4';
import type { SettingsPayload } from './types';
import { availablePayloadSchema, catalogStyles, type AvailableCourse, type CatalogRowState } from './addCourseCatalog';
import { AllTrackedNotice, CatalogList, CatalogToolbar } from './addCourseRows';
export function folderLabel(path: string | null | undefined): string | null | undefined {
  return (
    String(path || '')
      .split('/')
      .filter(Boolean)
      .pop() || path
  );
}

const errorBodySchema = z
  .object({
    detail: z.unknown(),
  })
  .partial();

const detailItemSchema = z
  .object({
    msg: z.string(),
  })
  .partial();

interface AddCourseResult {
  ok: boolean;
  message?: string;
}

async function postAddCourse(courseId: string, name: string, shortName: string): Promise<AddCourseResult> {
  const courseIdNumber = Number(courseId);
  const body = shortName
    ? JSON.stringify({ course_id: courseIdNumber, name, short_name: shortName })
    : JSON.stringify({ course_id: courseIdNumber, name });
  const response = await fetch('/api/v1/courses', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body,
  });
  if (response.ok) return { ok: true };
  let message = `Could not add course (HTTP ${response.status}).`;
  try {
    const parsed = errorBodySchema.safeParse(await response.json());
    if (parsed.success) {
      const detail = parsed.data.detail;
      if (Array.isArray(detail)) {
        message = detail
          .map((item) => {
            const parsedItem = detailItemSchema.safeParse(item);
            // SAFETY: error-body items are unknown JSON; msg is the documented string field.
            return parsedItem.success && parsedItem.data.msg ? parsedItem.data.msg : String(item);
          })
          .join('. ');
      } else if (detail) {
        message = String(detail);
      }
    }
  } catch {
    /* non-JSON error body */
  }
  return { ok: false, message };
}

async function loadCatalogCourses(knownIds: string[], signal: AbortSignal): Promise<AvailableCourse[]> {
  const response = await fetch('/api/v1/courses/available', {
    signal,
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    let message = `Could not load registered courses (HTTP ${response.status}).`;
    try {
      const parsed = errorBodySchema.safeParse(await response.json());
      if (parsed.success && parsed.data.detail) message = String(parsed.data.detail);
    } catch {
      /* keep status fallback */
    }
    throw new Error(message);
  }
  const parsed = availablePayloadSchema.safeParse(await response.json());
  if (!parsed.success) throw new Error('Unexpected registered-course response.');
  const known: Record<string, true> = {};
  for (const id of knownIds) known[id] = true;
  return parsed.data.courses.map((course) => ({
    id: String(course.id),
    code: course.code,
    title: course.title || course.name,
    professor: course.professor,
    name: course.name || course.title,
    registered: course.registered || Boolean(known[String(course.id)]),
  }));
}

function useAvailableCourses(knownIds: string[]) {
  const [courses, setCourses] = React.useState<AvailableCourse[] | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [catalogRevision, setCatalogRevision] = React.useState(0);
  const knownKey = knownIds.join(',');
  const load = React.useCallback(
    async (signal?: AbortSignal) => {
      setLoading(true);
      setError(null);
      try {
        const known = knownKey.split(',').filter(Boolean);
        setCourses(await loadCatalogCourses(known, signal ?? new AbortController().signal));
        setCatalogRevision((revision) => revision + 1);
      } catch (loadError) {
        if (loadError instanceof Error && loadError.name === 'AbortError') return;
        setError(loadError instanceof Error ? loadError.message : 'Could not load registered courses.');
      } finally {
        setLoading(false);
      }
    },
    [knownKey],
  );
  React.useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
  }, [load]);
  const markRegistered = React.useCallback((id: string, name: string) => {
    setCourses((current) =>
      (current || []).map((course) => (course.id === id ? { ...course, name, registered: true } : course)),
    );
  }, []);
  return { courses, loading, error, load, catalogRevision, markRegistered };
}

function useCatalogRows(courses: AvailableCourse[] | null, catalogRevision: number) {
  const [rows, setRows] = React.useState<Record<string, CatalogRowState>>({});
  React.useEffect(() => {
    setRows((current) => {
      const next: Record<string, CatalogRowState> = {};
      for (const course of courses || []) {
        const kept = current[course.id];
        next[course.id] = kept || { name: course.name, shortName: '', busy: false, error: null };
      }
      return next;
    });
  }, [courses, catalogRevision]);
  return [rows, setRows] as const;
}

function useFilteredCourses(courses: AvailableCourse[] | null, query: string) {
  return React.useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return courses || [];
    return (courses || []).filter((course) =>
      [course.title, course.name, course.code, course.professor, course.id].some((field) =>
        field.toLowerCase().includes(needle),
      ),
    );
  }, [courses, query]);
}

export interface AddCourseFormProps {
  data: SettingsPayload;
}

function useAddCatalogCourse(
  rows: Record<string, CatalogRowState>,
  patchRow: (id: string, patch: Partial<CatalogRowState>) => void,
  markRegistered: (id: string, name: string) => void,
  revalidate: () => void,
) {
  return React.useCallback(
    async (course: AvailableCourse) => {
      const row = rows[course.id];
      if (!row || row.busy) return;
      const name = row.name.trim();
      const shortName = row.shortName.trim();
      if (!name) {
        patchRow(course.id, { error: 'Course name is required.' });
        return;
      }
      patchRow(course.id, { busy: true, error: null });
      const result = await postAddCourse(course.id, name, shortName);
      if (result.ok) {
        patchRow(course.id, { busy: false, error: null });
        markRegistered(course.id, name);
        revalidate();
        return;
      }
      patchRow(course.id, { busy: false, error: result.message || 'Could not add course.' });
    },
    [markRegistered, patchRow, revalidate, rows],
  );
}
export function AddCourseForm({ data }: AddCourseFormProps) {
  const knownIds = React.useMemo(() => (data.courses || []).map((course) => String(course.id)), [data.courses]);
  const { courses, loading, error, load, catalogRevision, markRegistered } = useAvailableCourses(knownIds);
  const [rows, setRows] = useCatalogRows(courses, catalogRevision);
  const [query, setQuery] = React.useState('');
  const revalidator = useRevalidator();
  const revalidate = React.useCallback(() => void revalidator.revalidate(), [revalidator]);
  const patchRow = React.useCallback(
    (id: string, patch: Partial<CatalogRowState>) => {
      setRows((current) => {
        const kept = current[id];
        if (!kept) return current;
        return { ...current, [id]: { ...kept, ...patch } };
      });
    },
    [setRows],
  );
  const addCatalogCourse = useAddCatalogCourse(rows, patchRow, markRegistered, revalidate);
  const visible = useFilteredCourses(courses, query);
  const pending = visible.filter((course) => !course.registered);
  return (
    <AddCourseBody
      data={data}
      courses={courses}
      rows={rows}
      visible={visible}
      pendingCount={pending.length}
      loading={loading}
      error={error}
      query={query}
      onLoad={load}
      onQuery={setQuery}
      onChange={patchRow}
      onAdd={addCatalogCourse}
    />
  );
}

export interface AddCourseBodyProps {
  data: SettingsPayload;
  courses: AvailableCourse[] | null;
  rows: Record<string, CatalogRowState>;
  visible: AvailableCourse[];
  pendingCount: number;
  loading: boolean;
  error: string | null;
  query: string;
  onLoad: () => void;
  onQuery: (value: string) => void;
  onChange: (id: string, patch: Partial<CatalogRowState>) => void;
  onAdd: (course: AvailableCourse) => void;
}

export function AddCourseNotices({ data, error }: Pick<AddCourseBodyProps, 'data' | 'error'>) {
  return (
    <div {...stylex.props(catalogStyles.settingsFormGrid)}>
      <p {...stylex.props(catalogStyles.settingsSectionDescription)}>
        Registered courses come from your eClass portfolio. Names are editable before adding; the short name is
        optional. Files are synchronized during the next course check.
      </p>
      {!data.has_credentials && (
        <p {...stylex.props(catalogStyles.settingsFormError)} role="note">
          Save eClass credentials first, then refresh the catalog.
        </p>
      )}
      {error && (
        <p {...stylex.props(catalogStyles.settingsFormError)} role="alert">
          {error}
        </p>
      )}
    </div>
  );
}

export function AddCourseStates({
  courses,
  rows,
  visible,
  pendingCount,
  loading,
  onChange,
  onAdd,
}: Pick<AddCourseBodyProps, 'courses' | 'rows' | 'visible' | 'pendingCount' | 'loading' | 'onChange' | 'onAdd'>) {
  if (courses === null) {
    if (!loading) return null;
    return (
      <p {...stylex.props(catalogStyles.catalogEmpty)} role="status">
        Loading every course you are registered for on eClass.
      </p>
    );
  }
  if (visible.length === 0) {
    return (
      <p {...stylex.props(catalogStyles.catalogEmpty)} role="status">
        No registered courses match this filter.
      </p>
    );
  }
  return (
    <div {...stylex.props(catalogStyles.settingsFormGrid)}>
      <CatalogList courses={visible} rows={rows} onChange={onChange} onAdd={onAdd} />
      {pendingCount === 0 && <AllTrackedNotice />}
    </div>
  );
}

export function AddCourseBody(props: AddCourseBodyProps) {
  const { loading, loaded, query, onLoad, onQuery } = {
    loading: props.loading,
    loaded: props.courses !== null,
    query: props.query,
    onLoad: props.onLoad,
    onQuery: props.onQuery,
  };
  return (
    <div {...stylex.props(catalogStyles.settingsFormGrid)}>
      <CatalogToolbar loading={loading} loaded={loaded} query={query} onLoad={onLoad} onQuery={onQuery} />
      <AddCourseNotices data={props.data} error={props.error} />
      <AddCourseStates
        courses={props.courses}
        rows={props.rows}
        visible={props.visible}
        pendingCount={props.pendingCount}
        loading={props.loading}
        onChange={props.onChange}
        onAdd={props.onAdd}
      />
    </div>
  );
}
