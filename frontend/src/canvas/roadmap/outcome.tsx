import { useEffect, useRef, useState } from 'react';
import type { RoadmapAction } from '@/lib/types';
import { requestJson } from '../app/api';
import { publishCourseProgress } from '../app/events';
import { Button, Notice } from '../ui/controls';
import { Popover } from '../ui/overlays';
import { View } from '../ui/primitives';
import { buildRoadmapEvent } from './event';
import type { RecordedRoadmapEvent, RoadmapEventBody, RoadmapOutcome, RoadmapReflection } from './event';
import { ReflectionFields } from './reflection';
import { isRevisionConflict } from './resource';
import { styles } from './styles';

interface OutcomeProps {
  id: string;
  action: RoadmapAction;
  courseId: string | number;
  revision?: string;
  version: number;
  onRecorded: (outcome: RoadmapOutcome, saved: RecordedRoadmapEvent) => void;
  onRefresh: () => void;
}

const OUTCOMES: { event: RoadmapOutcome; label: string }[] = [
  { event: 'completed', label: 'Completed' },
  { event: 'partial', label: 'Partly done' },
  { event: 'stuck', label: 'Stuck' },
  { event: 'deferred', label: 'Do later' },
];

function useOutcomeAdmission(version: number, setError: (error: string | null) => void) {
  const [rejectedVersion, setRejectedVersion] = useState<number | null>(null);
  const acceptedVersion = useRef(version);
  acceptedVersion.current = version;
  const stale = rejectedVersion !== null && version <= rejectedVersion;
  useEffect(() => {
    if (rejectedVersion !== null && version > rejectedVersion) {
      setRejectedVersion(null);
      setError(null);
    }
  }, [version, rejectedVersion, setError]);
  return { stale, reject: () => setRejectedVersion(acceptedVersion.current) };
}

function initialReflection(action: RoadmapAction): RoadmapReflection {
  return {
    minutes: String(action.remaining_minutes || action.estimated_minutes || 15),
    confidence: '',
    note: '',
  };
}

function useOutcome(props: OutcomeProps) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const admission = useOutcomeAdmission(props.version, setError);
  const stale = admission.stale;
  const [reflection, setReflection] = useState<RoadmapReflection>(() => initialReflection(props.action));
  const intents = useRef(new Map<RoadmapOutcome, RoadmapEventBody>());
  const locked = useRef(false);
  const record = async (eventType: RoadmapOutcome) => {
    if (locked.current || stale) return;
    const previous = intents.current.get(eventType);
    const result = previous
      ? { body: previous }
      : buildRoadmapEvent(props.action, props.courseId, props.revision, eventType, reflection);
    if ('error' in result) {
      setError(result.error);
      return;
    }
    intents.current.set(eventType, result.body);
    locked.current = true;
    setBusy(true);
    setError(null);
    let saved: RecordedRoadmapEvent;
    try {
      saved = await requestJson<RecordedRoadmapEvent>('/api/v1/study/actions/event', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(result.body),
      });
    } catch (failure) {
      const parsed = failure instanceof Error ? failure : new Error('Could not record progress');
      setError(parsed.message);
      // A GET accepted before this rejection cannot reopen admission.
      if (isRevisionConflict(parsed)) admission.reject();
      return;
    } finally {
      locked.current = false;
      setBusy(false);
    }
    intents.current.delete(eventType);
    props.onRecorded(eventType, saved);
    if (saved.overview) publishCourseProgress(saved.overview);
    else props.onRefresh();
  };
  return { busy, error, stale, reflection, setReflection, record, hasRetry: intents.current.size > 0 };
}

function OutcomeFields({
  id,
  state,
  onRefresh,
}: {
  id: string;
  state: ReturnType<typeof useOutcome>;
  onRefresh: () => void;
}) {
  return (
    <View style={styles.stack}>
      <View style={styles.row}>
        {OUTCOMES.map(({ event, label }) => (
          <Button
            key={event}
            id={`${id}:outcome:${event}`}
            disabled={state.busy || state.stale}
            onClick={() => {
              void state.record(event);
            }}
          >
            {state.busy ? 'Saving…' : label}
          </Button>
        ))}
      </View>
      <ReflectionFields
        id={id}
        value={state.reflection}
        onChange={state.setReflection}
        disabled={state.busy || state.stale}
      />
      {state.error && <Notice tone="danger">{state.error}</Notice>}
      {state.stale && (
        <Button id={`${id}:refresh`} onClick={onRefresh}>
          Refresh roadmap
        </Button>
      )}
      {state.error && state.hasRetry && !state.stale && (
        <Notice>Retrying the same outcome uses its original reflection and request ID.</Notice>
      )}
    </View>
  );
}

export function RoadmapOutcomeControls(props: OutcomeProps) {
  const [open, setOpen] = useState(false);
  const state = useOutcome(props);
  const anchorId = `${props.id}:finish`;
  return (
    <View style={styles.stack}>
      <Button
        id={anchorId}
        expanded={open}
        controls={`${props.id}:finish-options`}
        onClick={() => setOpen(!open)}
        disabled={!props.revision}
      >
        Finish
      </Button>
      <Popover
        id={`${props.id}:finish-options`}
        anchorId={anchorId}
        title="Finish action"
        open={open}
        onClose={() => setOpen(false)}
      >
        <OutcomeFields id={props.id} state={state} onRefresh={props.onRefresh} />
      </Popover>
      {!open && state.error && <Notice tone="danger">{state.error}</Notice>}
    </View>
  );
}
