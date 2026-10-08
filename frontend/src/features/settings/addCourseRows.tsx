import * as stylex from '@stylexjs/stylex';
import { buttonStyles } from '@/components/ui/styles';
import { catalogStyles, type AvailableCourse, type CatalogRowState } from './addCourseCatalog';

export interface CatalogRowProps {
  course: AvailableCourse;
  row: CatalogRowState;
  onChange: (patch: Partial<CatalogRowState>) => void;
  onAdd: () => void;
}

export function CatalogFields({
  course,
  row,
  onChange,
}: {
  course: AvailableCourse;
  row: CatalogRowState;
  onChange: (patch: Partial<CatalogRowState>) => void;
}) {
  return (
    <div {...stylex.props(catalogStyles.catalogFields)}>
      <label>
        <span {...stylex.props(catalogStyles.catalogMeta)}>Course name</span>
        <input
          {...stylex.props(catalogStyles.catalogInput)}
          name={`available_name_${course.id}`}
          value={row.name}
          maxLength={200}
          onChange={(event) => onChange({ name: event.target.value })}
          lang="el"
        />
      </label>
      <label>
        <span {...stylex.props(catalogStyles.catalogMeta)}>Short name (optional)</span>
        <input
          {...stylex.props(catalogStyles.catalogInput)}
          name={`available_short_${course.id}`}
          value={row.shortName}
          maxLength={24}
          placeholder="ΔΣ"
          onChange={(event) => onChange({ shortName: event.target.value })}
          lang="el"
        />
      </label>
    </div>
  );
}

export function CatalogRowHeader({ course }: { course: AvailableCourse }) {
  return (
    <div>
      <p {...stylex.props(catalogStyles.catalogTitle)} lang="el">
        {course.title}
      </p>
      <span {...stylex.props(catalogStyles.catalogMeta)}>
        {course.code} · ID {course.id}
        {course.professor ? ` · ${course.professor}` : ''}
      </span>
    </div>
  );
}

export function CatalogRowError({ error }: { error: string | null }) {
  if (!error) return null;
  return (
    <span {...stylex.props(catalogStyles.settingsFormError)} role="alert">
      {error}
    </span>
  );
}

export function CatalogRow({ course, row, onChange, onAdd }: CatalogRowProps) {
  return (
    <li {...stylex.props(catalogStyles.catalogRow)}>
      <div>
        <CatalogRowHeader course={course} />
        <CatalogFields course={course} row={row} onChange={onChange} />
        <CatalogRowError error={row.error} />
      </div>
      <CatalogRowAction course={course} row={row} onAdd={onAdd} />
    </li>
  );
}

export function CatalogRowAction({
  course,
  row,
  onAdd,
}: {
  course: AvailableCourse;
  row: CatalogRowState;
  onAdd: () => void;
}) {
  return (
    <div {...stylex.props(catalogStyles.catalogActions)}>
      {course.registered ? (
        <span {...stylex.props(catalogStyles.registeredBadge)}>Added</span>
      ) : (
        <button
          {...stylex.props(buttonStyles.base, buttonStyles.primary)}
          type="button"
          disabled={row.busy || !row.name.trim()}
          onClick={onAdd}
        >
          {row.busy ? 'Adding…' : 'Add'}
        </button>
      )}
    </div>
  );
}

export function CatalogList({
  courses,
  rows,
  onChange,
  onAdd,
}: {
  courses: AvailableCourse[];
  rows: Record<string, CatalogRowState>;
  onChange: (id: string, patch: Partial<CatalogRowState>) => void;
  onAdd: (course: AvailableCourse) => void;
}) {
  return (
    <ol {...stylex.props(catalogStyles.catalogList)} aria-label="Registered eClass courses">
      {courses.map((course) => (
        <CatalogRow
          key={course.id}
          course={course}
          row={rows[course.id] || { name: course.name, shortName: '', busy: false, error: null }}
          onChange={(patch) => onChange(course.id, patch)}
          onAdd={() => onAdd(course)}
        />
      ))}
    </ol>
  );
}

export function CatalogToolbar({
  loading,
  loaded,
  query,
  onLoad,
  onQuery,
}: {
  loading: boolean;
  loaded: boolean;
  query: string;
  onLoad: () => void;
  onQuery: (value: string) => void;
}) {
  return (
    <div {...stylex.props(catalogStyles.catalogToolbar)}>
      {loaded ? (
        <input
          {...stylex.props(catalogStyles.catalogSearch)}
          type="search"
          aria-label="Filter registered courses"
          placeholder="Filter registered courses…"
          value={query}
          onChange={(event) => onQuery(event.target.value)}
        />
      ) : (
        <p {...stylex.props(catalogStyles.catalogEmpty)} role="status">
          {loading ? 'Loading registered courses…' : 'Registered courses will load automatically.'}
        </p>
      )}
      {loaded && (
        <button
          {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
          type="button"
          onClick={onLoad}
          disabled={loading}
        >
          {loading ? 'Refreshing…' : 'Refresh'}
        </button>
      )}
    </div>
  );
}

export function AllTrackedNotice() {
  return (
    <p {...stylex.props(catalogStyles.settingsFormSuccess)} role="status">
      Every registered course is already tracked.
    </p>
  );
}
