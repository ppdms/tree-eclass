import { useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router';
import { requestJson } from '../app/api';
import { publishCatalogChanged } from '../app/events';
import { Button, Notice, Panel } from '../ui/controls';
import { Field } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import { Section } from './section';
import { useCatalog } from './catalog';
import type { CatalogModel } from './catalog';
import type { AvailableCourse, CatalogRowState } from './catalogState';
import { Hint, message, styles, useSettingsContext } from './shared';
import type { SettingsCourse, SettingsPayload } from './shared';

function CatalogRow({ course, row, model }: { course: AvailableCourse; row: CatalogRowState; model: CatalogModel }) {
  return (
    <Panel id={`settings-available-${course.id}`}>
      <Text style={styles.heading}>{course.title}</Text>
      <Hint>{`${course.code} · ID ${course.id}${course.professor ? ` · ${course.professor}` : ''}`}</Hint>
      <Field
        id={`available_name_${course.id}`}
        label="Course name"
        value={row.name}
        maxLength={200}
        onChange={(name) => model.patch(course.id, { name })}
        onSubmit={() => void model.add(course)}
      />
      <Field
        id={`available_short_${course.id}`}
        label="Short name (optional)"
        value={row.shortName}
        maxLength={24}
        placeholder="ΔΣ"
        onChange={(shortName) => model.patch(course.id, { shortName })}
        onSubmit={() => void model.add(course)}
      />
      {row.error && <Notice tone="danger">{row.error}</Notice>}
      {course.registered ? (
        <Text style={styles.muted}>Added</Text>
      ) : (
        <Button
          id={`settings-add-${course.id}`}
          disabled={row.busy || !row.name.trim()}
          tone="primary"
          onClick={() => void model.add(course)}
        >
          {row.busy ? 'Adding…' : 'Add'}
        </Button>
      )}
    </Panel>
  );
}
function CatalogToolbar({
  model,
  query,
  onQuery,
}: {
  model: CatalogModel;
  query: string;
  onQuery(value: string): void;
}) {
  return (
    <View style={styles.row}>
      {model.courses ? (
        <>
          <Field
            id="settings-course-filter"
            type="search"
            label="Filter registered courses"
            placeholder="Filter registered courses…"
            value={query}
            onChange={onQuery}
          />
          <Button id="settings-catalog-refresh" disabled={model.loading} onClick={() => void model.load()}>
            {model.loading ? 'Refreshing…' : 'Refresh'}
          </Button>
        </>
      ) : (
        <Hint>{model.loading ? 'Loading registered courses…' : 'Registered courses will load automatically.'}</Hint>
      )}
    </View>
  );
}
function CatalogContents({ model, visible }: { model: CatalogModel; visible: AvailableCourse[] }) {
  return (
    <View style={styles.stack}>
      {model.courses === null && model.loading && <Hint>Loading every course you are registered for on eClass.</Hint>}
      {model.courses &&
        (visible.length ? (
          visible.map((course) => (
            <CatalogRow key={course.id} course={course} row={model.rows[course.id]!} model={model} />
          ))
        ) : (
          <Hint>No registered courses match this filter.</Hint>
        ))}
      {visible.length > 0 && visible.every((course) => course.registered) && (
        <Notice tone="success">Every registered course is already tracked.</Notice>
      )}
    </View>
  );
}
function AddCourse({ data }: { data: SettingsPayload }) {
  const known = useMemo(() => (data.courses ?? []).map((course) => String(course.id)), [data.courses]);
  const model = useCatalog(known);
  const [query, setQuery] = useState('');
  const needle = query.trim().toLowerCase();
  const visible = (model.courses ?? []).filter(
    (course) =>
      !needle ||
      [course.title, course.name, course.code, course.professor, course.id].some((text) =>
        text.toLowerCase().includes(needle),
      ),
  );
  return (
    <View style={styles.stack}>
      <CatalogToolbar model={model} query={query} onQuery={setQuery} />
      <Hint>Registered courses come from your eClass portfolio. Names are editable; short names are optional.</Hint>
      <Hint>Files are synchronized during the next course check.</Hint>
      {!data.has_credentials && (
        <Notice tone="warning">Save eClass credentials first, then refresh the catalog.</Notice>
      )}
      {model.error && <Notice tone="danger">{model.error}</Notice>}
      <CatalogContents model={model} visible={visible} />
    </View>
  );
}
function useRestoreCourse(id: number | string) {
  const context = useSettingsContext();
  const navigate = useNavigate();
  const inFlight = useRef(false);
  const [busy, setBusy] = useState(false);
  const [restored, setRestored] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const show = async () => {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    setError(null);
    try {
      await requestJson(`/api/v1/courses/${encodeURIComponent(String(id))}/show`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });
      setRestored(true);
      context.refresh();
      publishCatalogChanged();
      void navigate('/settings#hidden-courses');
    } catch (failure) {
      setError(message(failure, 'Could not restore this course.'));
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  };
  return { busy, restored, error, show };
}
function HiddenCourse({ course }: { course: SettingsCourse }) {
  const { busy, restored, error, show } = useRestoreCourse(course.id);
  const folder = course.storage_prefix ?? '';
  return (
    <Panel id={`settings-hidden-${course.id}`}>
      <Text style={styles.heading}>{course.name}</Text>
      {folder && (
        <Text title={folder} style={styles.muted}>
          {folder.split('/').filter(Boolean).pop() || folder}
        </Text>
      )}
      <Button
        id={`settings-show-${course.id}`}
        disabled={busy || restored}
        label={`Show ${course.name}`}
        onClick={() => void show()}
      >
        {restored ? 'Restored' : busy ? 'Restoring…' : 'Show'}
      </Button>
      {error && <Notice tone="danger">{error}</Notice>}
    </Panel>
  );
}
export function CourseSections({ data }: { data: SettingsPayload }) {
  return (
    <>
      <Section
        id="add-course"
        title="Add course"
        status="New download"
        description="Course checks download its files into local document storage."
      >
        <AddCourse data={data} />
      </Section>
      <Section
        id="hidden-courses"
        title="Hidden courses"
        description={
          data.hidden_courses?.length
            ? 'Hidden courses remain synchronized but stay out of the daily workspace.'
            : 'No hidden courses.'
        }
      >
        {(data.hidden_courses ?? []).map((course) => (
          <HiddenCourse key={course.id} course={course} />
        ))}
      </Section>
    </>
  );
}
