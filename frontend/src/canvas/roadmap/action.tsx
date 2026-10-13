import { useEffect, useState } from 'react';
import type { RoadmapAction } from '@/lib/types';
import { readableValue } from '@/lib/display';
import { AppLink } from '../app/links';
import { Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { Evidence } from './evidence';
import { RoadmapOutcomeControls } from './outcome';
import type { RecordedRoadmapEvent, RoadmapOutcome } from './event';
import { semanticId, styles } from './styles';

interface ActionCardProps {
  action: RoadmapAction;
  courseId: string | number;
  revision?: string;
  version: number;
  onRefresh: () => void;
}

export function ActionCard({ action, courseId, revision, version, onRefresh }: ActionCardProps) {
  const [status, setStatus] = useState(action.status);
  const [recorded, setRecorded] = useState('');
  const [refreshError, setRefreshError] = useState<string | null>(null);
  useEffect(() => setStatus(action.status), [action.status]);
  const actionId = action.action_id || String(action.id || '');
  const id = semanticId('roadmap', courseId, revision, 'action', action.unit_key, actionId);
  const completed = status === 'completed';
  const onRecorded = (outcome: RoadmapOutcome, saved: RecordedRoadmapEvent) => {
    setStatus(outcome);
    setRecorded(`Recorded: ${outcome}`);
    setRefreshError(saved.overview_error || (saved.overview ? null : 'Progress saved. Overview refresh is pending.'));
  };
  return (
    <View id={id} style={{ ...styles.action, opacity: completed ? 0.7 : 1 }}>
      <View style={styles.row}>
        <Text style={{ ...styles.heading, color: completed ? colors.success : colors.text }}>
          {completed ? '✓' : '○'} {readableValue(action.kind || action.action_type, 'Study')}
        </Text>
        <Text style={styles.meta}>{action.remaining_minutes || action.estimated_minutes || 15} min</Text>
        <Evidence id={`${id}:evidence`} links={action.evidence_links} courseId={courseId} actionId={actionId} />
      </View>
      <Text style={styles.body}>{readableValue(action.instruction)}</Text>
      {!completed && (
        <ActionControls
          id={id}
          action={action}
          courseId={courseId}
          revision={revision}
          version={version}
          onRecorded={onRecorded}
          onRefresh={onRefresh}
        />
      )}
      {recorded && <Notice tone="success">{recorded}</Notice>}
      {refreshError && <Notice tone="warning">{refreshError}</Notice>}
    </View>
  );
}

function ActionControls({
  id,
  action,
  courseId,
  revision,
  version,
  onRecorded,
  onRefresh,
}: {
  id: string;
  action: RoadmapAction;
  courseId: string | number;
  revision?: string;
  version: number;
  onRecorded: (outcome: RoadmapOutcome, saved: RecordedRoadmapEvent) => void;
  onRefresh: () => void;
}) {
  const actionId = action.action_id || String(action.id || '');
  return (
    <View style={styles.row}>
      <AppLink
        id={`${id}:start`}
        href={`/study/session?course_id=${encodeURIComponent(courseId)}&action_id=${encodeURIComponent(actionId)}`}
        style={{ ...styles.body, color: colors.link }}
      >
        Start
      </AppLink>
      <RoadmapOutcomeControls
        id={id}
        action={action}
        courseId={courseId}
        revision={revision}
        version={version}
        onRecorded={onRecorded}
        onRefresh={onRefresh}
      />
    </View>
  );
}
