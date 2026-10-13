import { useState } from 'react';
import type { RoadmapBlueprint, RoadmapUnit } from '@/lib/types';
import { Button, Notice, Panel, Progress } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { ActionCard } from './action';
import { Evidence } from './evidence';
import { isRevisionConflict, useRoadmapResource } from './resource';
import { semanticId, styles } from './styles';

type Roadmap = NonNullable<RoadmapBlueprint['blueprint']>;
interface UnitProps {
  unit: RoadmapUnit;
  courseId: string | number;
  revision?: string;
  version: number;
  onRefresh: () => void;
}

function UnitContent({ unit, courseId, revision, version, onRefresh }: UnitProps) {
  const id = semanticId('roadmap', courseId, revision, 'unit', unit.key || unit.title);
  return (
    <View style={styles.stack}>
      {unit.objective && <Text style={styles.body}>{unit.objective}</Text>}
      <Evidence id={`${id}:evidence`} links={unit.evidence_links} courseId={courseId} />
      {(unit.actions || []).map((action) => (
        <ActionCard
          key={semanticId(revision, action.unit_key, action.action_id || action.id)}
          action={action}
          courseId={courseId}
          revision={revision}
          version={version}
          onRefresh={onRefresh}
        />
      ))}
    </View>
  );
}

function DeferredUnit(props: UnitProps & { open: boolean }) {
  const { unit, revision, courseId, open } = props;
  const id = semanticId('roadmap', courseId, revision, 'unit', unit.key || unit.title);
  const url = `/api/v1/courses/${encodeURIComponent(courseId)}/roadmap/units/${encodeURIComponent(
    unit.key || '',
  )}?revision=${encodeURIComponent(revision || '')}`;
  const resource = useRoadmapResource<{ unit: RoadmapUnit; revision_id?: string }>(
    url,
    open && !unit.actions && Boolean(revision),
    props.version,
  );
  if (!open) return null;
  const stale =
    isRevisionConflict(resource.error) || Boolean(resource.data?.revision_id && resource.data.revision_id !== revision);
  if (stale || (!revision && !unit.actions))
    return (
      <Notice tone="warning">
        <Text>The roadmap changed. Refresh it to open the current topic.</Text>
        <Button id={`${id}:refresh`} onClick={props.onRefresh}>
          Refresh roadmap
        </Button>
      </Notice>
    );
  // Deferred action admission follows the unit GET, not the parent refresh.
  const content = unit.actions ? unit : resource.data?.unit;
  return (
    <View style={styles.stack}>
      {resource.error && (
        <Notice tone="danger">
          <Text>{`Topic could not load. ${resource.error.message}`}</Text>
          <Button id={`${id}:retry`} onClick={resource.retry}>
            Try again
          </Button>
        </Notice>
      )}
      {resource.pending && <Text style={styles.meta}>Loading topic…</Text>}
      {content && (
        <UnitContent {...props} unit={content} version={unit.actions ? props.version : resource.generation} />
      )}
    </View>
  );
}

function UnitItem(props: UnitProps & { ordinal: number; initialOpen: boolean }) {
  const [open, setOpen] = useState(props.initialOpen);
  const unit = props.unit;
  const id = semanticId('roadmap', props.courseId, props.revision, 'unit', unit.key || unit.title);
  const completed = Number(unit.completed_actions || 0);
  const total = Number(unit.total_actions || 0);
  const percent = total ? Math.round((completed / total) * 100) : 0;
  const priorityColor =
    unit.priority === 'critical' ? colors.danger : unit.priority === 'high' ? colors.warning : colors.muted;
  return (
    <Panel id={id} style={styles.card}>
      <Button
        id={`${id}:toggle`}
        expanded={open}
        controls={`${id}:body`}
        style={styles.disclosure}
        onClick={() => setOpen(!open)}
      >
        <Text style={styles.heading}>
          {completed === total && total > 0 ? '✓' : props.ordinal} {unit.title || 'Topic'} {open ? '⌃' : '⌄'}
        </Text>
      </Button>
      <View style={styles.row}>
        {unit.priority && <Text style={{ ...styles.meta, color: priorityColor }}>{unit.priority}</Text>}
        <Text style={styles.meta}>
          {completed}/{total} · {unit.estimated_minutes || 0}m
        </Text>
      </View>
      <Progress id={`${id}:progress`} value={percent} max={100} label={`${percent}% complete`} />
      <View id={`${id}:body`}>
        <DeferredUnit {...props} open={open} />
      </View>
    </Panel>
  );
}

export function Units({
  roadmap,
  courseId,
  revision,
  version,
  onRefresh,
}: {
  roadmap: Roadmap;
  courseId: string | number;
  revision?: string;
  version: number;
  onRefresh: () => void;
}) {
  const units = roadmap.units || [];
  const current = units.findIndex((unit) => Number(unit.completed_actions || 0) < Number(unit.total_actions || 0));
  return (
    <View style={styles.stack}>
      {units.map((unit, index) => (
        <UnitItem
          key={semanticId(revision, unit.key || unit.title)}
          unit={unit}
          ordinal={index + 1}
          initialOpen={index === Math.max(0, current)}
          courseId={courseId}
          revision={revision}
          version={version}
          onRefresh={onRefresh}
        />
      ))}
    </View>
  );
}
