import * as React from 'react';
import * as stylex from '@stylexjs/stylex';
import { courseDetailStyles as styles } from './courseDetailStyles';
import { Button } from '@/components/ui/button';
import { fetchJson } from '@/lib/api';
import type { CourseDetailPayload, CourseTimelineItem } from '@/lib/types';
import CourseRoadmap, { PracticePanel } from './course-roadmap';
import { RecentChanges } from './courseDetailSections';

function expandRoadmap(data: CourseDetailPayload) {
  const view = data.course_blueprint;
  if (!view || view.actions_deferred) return data;
  const actions = new Map(view.actions?.map((action) => [action.action_id, action]));
  for (const unit of view.blueprint?.units || []) {
    unit.actions = (unit.action_ids || []).flatMap((id) => actions.get(id) || []);
  }
  if (view.progress) view.progress.next_action = actions.get(view.progress.next_action_id);
  return data;
}

function useRoadmap(courseId: string | number) {
  const [data, setData] = React.useState<CourseDetailPayload | null>(null);
  const [error, setError] = React.useState(false);
  const [version, retry] = React.useReducer((value: number) => value + 1, 0);
  React.useEffect(() => {
    const update = () => retry();
    window.addEventListener('tree:course-progress', update);
    return () => window.removeEventListener('tree:course-progress', update);
  }, []);
  React.useEffect(() => {
    const controller = new AbortController();
    setError(false);
    fetchJson<CourseDetailPayload>(`api/v1/courses/${courseId}/roadmap?include_actions=false`, {
      signal: controller.signal,
    })
      .then((value) => setData(expandRoadmap(value)))
      .catch(() => {
        if (!controller.signal.aborted) setError(true);
      });
    return () => controller.abort();
  }, [courseId, version]);
  return { data, error, retry };
}

export function LazyRoadmap({ courseId }: { courseId: string | number }) {
  const { data, error, retry } = useRoadmap(courseId);
  if (error)
    return (
      <p role="alert">
        Roadmap could not load. <Button onClick={retry}>Try again</Button>
      </p>
    );
  if (!data) return <p role="status">Loading roadmap…</p>;
  return (
    <>
      <CourseRoadmap course={data.course} blueprint={data.course_blueprint} />
      {data.course_blueprint?.practice?.enabled && <PracticeDisclosure data={data} />}
    </>
  );
}

function PracticeDisclosure({ data }: { data: CourseDetailPayload }) {
  const [open, setOpen] = React.useState(false);
  return (
    <details {...stylex.props(styles.coursePracticeDisclosure)} onToggle={(event) => setOpen(event.currentTarget.open)}>
      <summary {...stylex.props(styles.coursePracticeDisclosureSummary)}>Practice questions</summary>
      {open && data.course_blueprint?.practice && (
        <PracticePanel course={data.course} practice={data.course_blueprint.practice} />
      )}
    </details>
  );
}

interface Updates {
  timeline: CourseTimelineItem[];
  has_more: boolean;
}

export function LazyUpdates({ courseId }: { courseId: string | number }) {
  const [items, setItems] = React.useState<CourseTimelineItem[]>([]);
  const [offset, setOffset] = React.useState(0);
  const [hasMore, setHasMore] = React.useState(false);
  const [busy, setBusy] = React.useState(true);
  const [error, setError] = React.useState(false);
  const [version, retry] = React.useReducer((value: number) => value + 1, 0);
  React.useEffect(() => {
    const controller = new AbortController();
    setBusy(true);
    setError(false);
    fetchJson<Updates>(`api/v1/courses/${courseId}/updates?limit=10&offset=${offset}`, { signal: controller.signal })
      .then((data) => {
        setItems((previous) => (offset ? [...previous, ...data.timeline] : data.timeline));
        setHasMore(data.has_more);
      })
      .catch(() => {
        if (!controller.signal.aborted) setError(true);
      })
      .finally(() => {
        if (!controller.signal.aborted) setBusy(false);
      });
    return () => controller.abort();
  }, [courseId, offset, version]);
  return (
    <>
      <RecentChanges items={items} courseId={courseId} />
      {busy && <p role="status">Loading updates…</p>}
      {error && (
        <p role="alert">
          Updates could not load. <Button onClick={retry}>Try again</Button>
        </p>
      )}
      {!error && hasMore && (
        <Button disabled={busy} onClick={() => setOffset(items.length)}>
          Load more
        </Button>
      )}
    </>
  );
}
