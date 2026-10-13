import { useEffect, useMemo, useReducer } from 'react';
import type { CourseDetailPayload, RoadmapBlueprint } from '@/lib/types';
import { subscribeCourseProgress } from '../app/events';
import { Button, Notice, Panel, Progress } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { PracticeDisclosure } from '../practice';
import { useRoadmapResource } from './resource';
import { StrategySection } from './strategy';
import { SupportSection } from './support';
import { Units } from './units';
import { RoadmapWaiting } from './waiting';
import { semanticId, styles } from './styles';

export { RoadmapReadinessNote } from './waiting';
export { PracticePanel } from '../practice';

function expandRoadmap(data: CourseDetailPayload): CourseDetailPayload {
  const view = data.course_blueprint;
  if (!view || view.actions_deferred) return data;
  const actions = new Map((view.actions || []).map((action) => [action.action_id, action]));
  const units = (view.blueprint?.units || []).map((unit) =>
    unit.actions
      ? unit
      : {
          ...unit,
          actions: (unit.action_ids || []).flatMap((id) => actions.get(id) || []),
        },
  );
  return {
    ...data,
    course_blueprint: {
      ...view,
      blueprint: view.blueprint ? { ...view.blueprint, units } : undefined,
      progress: view.progress
        ? {
            ...view.progress,
            next_action: actions.get(view.progress.next_action_id) || view.progress.next_action,
          }
        : undefined,
    },
  };
}

function RoadmapHeader({ blueprint, id }: { blueprint: RoadmapBlueprint; id: string }) {
  const progress = blueprint.progress || {};
  const percent = Math.round(Number(progress.percent || 0));
  const revision = blueprint.revision_id || blueprint.revision_hash;
  return (
    <Panel style={styles.card}>
      <Text style={styles.meta}>Learning path</Text>
      <Text style={styles.title}>Roadmap</Text>
      <Text style={styles.heading}>{percent}%</Text>
      <Text style={styles.meta}>
        {progress.completed_actions || 0}/{progress.total_actions || 0} actions
      </Text>
      <Progress id={`${id}:progress`} value={Math.min(100, percent)} max={100} label={`${percent}% complete`} />
      {revision && <Text style={styles.meta}>Current revision: {revision}</Text>}
      {blueprint.readiness?.refresh_pending && (
        <Notice>A refreshed roadmap is being prepared. This validated revision remains available.</Notice>
      )}
    </Panel>
  );
}

function RoadmapSecondary(props: Parameters<typeof StrategySection>[0]) {
  return (
    <View style={styles.secondary}>
      <StrategySection {...props} />
      <SupportSection {...props} />
    </View>
  );
}

function RoadmapContents({
  blueprint,
  courseId,
  version,
  onRefresh,
}: {
  blueprint?: RoadmapBlueprint;
  courseId: string | number;
  version: number;
  onRefresh: () => void;
}) {
  const roadmap = blueprint?.blueprint;
  const revision = blueprint?.revision_id || blueprint?.revision_hash;
  const id = semanticId('roadmap', courseId, revision);
  return (
    <View style={styles.stack}>
      {!roadmap || !blueprint || blueprint.usable === false ? (
        <RoadmapWaiting blueprint={blueprint} />
      ) : (
        <View style={styles.stack}>
          <RoadmapHeader id={id} blueprint={blueprint} />
          <Units roadmap={roadmap} courseId={courseId} revision={revision} version={version} onRefresh={onRefresh} />
          <RoadmapSecondary
            key={revision}
            roadmap={roadmap}
            courseId={courseId}
            revision={revision}
            version={version}
            onRefresh={onRefresh}
          />
        </View>
      )}
      {blueprint?.practice?.enabled && (
        <PracticeDisclosure
          key={revision}
          courseId={courseId}
          practice={blueprint.practice}
          revision={revision}
          version={version}
          onRefresh={onRefresh}
        />
      )}
    </View>
  );
}

function RoadmapCourse({ courseId }: { courseId: string | number }) {
  const [version, refresh] = useReducer((value: number) => value + 1, 0);
  const url = `/api/v1/courses/${encodeURIComponent(courseId)}/roadmap?include_actions=false`;
  const resource = useRoadmapResource<CourseDetailPayload>(url, true, version);
  const data = useMemo(() => (resource.data ? expandRoadmap(resource.data) : null), [resource.data]);
  useEffect(
    () =>
      subscribeCourseProgress((overview) => {
        if (String(overview.course.id) === String(courseId)) refresh();
      }),
    [courseId],
  );
  const id = semanticId('roadmap', courseId);
  return (
    <View id={id} style={styles.stack}>
      {resource.error && (
        <Notice tone="danger">
          <Text>{`Roadmap could not load. ${resource.error.message}`}</Text>
          {data && <Text>The previously validated roadmap is retained; no refreshed evidence has been published.</Text>}
          <Button id={`${id}:retry`} onClick={refresh}>
            Try again
          </Button>
        </Notice>
      )}
      {resource.pending && <Text style={styles.meta}>{data ? 'Refreshing roadmap…' : 'Loading roadmap…'}</Text>}
      {data && (
        <RoadmapContents
          blueprint={data.course_blueprint}
          courseId={courseId}
          version={resource.generation}
          onRefresh={refresh}
        />
      )}
    </View>
  );
}

export function CourseRoadmap({ courseId }: { courseId: string | number }) {
  return <RoadmapCourse key={String(courseId)} courseId={courseId} />;
}
