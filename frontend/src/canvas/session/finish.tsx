import { useEffect, useState } from 'react';
import { AppLink } from '../app/links';
import { Button, Notice } from '../ui/controls';
import { Field, Select } from '../ui/forms';
import { Popover } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';
import type { Outcome } from './types';
import type { FinishState } from './useFinish';
import type { SittingState } from './useSitting';

const outcomes: { value: Outcome; label: string }[] = [
  { value: 'completed', label: 'Done' },
  { value: 'partial', label: 'Partial' },
  { value: 'stuck', label: 'Stuck' },
  { value: 'deferred', label: 'Defer' },
];
const confidenceOptions = [
  { value: '', label: 'Not specified' },
  ...[0, 1, 2, 3, 4, 5].map((value) => ({ value: String(value), label: `${value} / 5` })),
];
interface FinishPopoverProps {
  open: boolean;
  onClose: () => void;
  action: boolean;
  sitting: SittingState;
  state: FinishState;
  questions: number;
}
interface FinishReflectionProps {
  state: FinishState;
  note: string;
  confidence: string;
  setNote: (value: string) => void;
  setConfidence: (value: string) => void;
}
function FinishReflection({ state, note, confidence, setNote, setConfidence }: FinishReflectionProps) {
  return (
    <>
      <Text style={styles.body}>
        Record what actually happened. Reading and recall never complete an action for you.
      </Text>
      <Field
        id="session-finish-reflection"
        label="Reflection (optional)"
        value={state.intent?.note ?? note}
        onChange={setNote}
        readOnly={Boolean(state.intent)}
        multiline
        rows={3}
        disabled={state.busy}
      />
      <Select
        id="session-finish-confidence"
        label="Confidence (optional)"
        value={
          state.intent?.confidence === null || state.intent?.confidence === undefined
            ? confidence
            : String(state.intent.confidence)
        }
        onChange={setConfidence}
        disabled={state.busy || Boolean(state.intent)}
        options={confidenceOptions}
      />
    </>
  );
}
export function FinishPopover(props: FinishPopoverProps) {
  const [note, setNote] = useState('');
  const [confidence, setConfidence] = useState('');
  useEffect(() => {
    setNote('');
    setConfidence('');
  }, [props.sitting.session?.id]);
  const closed = props.state.result?.session ?? (props.sitting.session?.ended_at ? props.sitting.session : null);
  return (
    <Popover
      id="session-finish-menu"
      title="Finish session"
      open={props.open}
      anchorId="session-finish"
      onClose={props.onClose}
    >
      <View style={styles.column}>
        {closed ? (
          <RecordedOutcome {...props} />
        ) : (
          <>
            <FinishReflection
              state={props.state}
              note={note}
              confidence={confidence}
              setNote={setNote}
              setConfidence={setConfidence}
            />
            <FinishOutcomes {...props} note={note} confidence={confidence} />
          </>
        )}
        {props.state.error && <Notice tone="danger">{props.state.error}</Notice>}
        {props.state.warning && <Notice tone="warning">{props.state.warning}</Notice>}
      </View>
    </Popover>
  );
}
function FinishOutcomes(props: {
  action: boolean;
  sitting: SittingState;
  state: FinishState;
  note: string;
  confidence: string;
}) {
  if (props.state.intent)
    return (
      <View style={styles.column}>
        <Text style={styles.body}>
          Pending outcome: {props.state.intent.outcome}. Retry preserves this exact outcome and reflection.
        </Text>
        <Button
          id="session-finish-retry"
          disabled={props.state.busy}
          onClick={() => {
            void props.state.retry();
          }}
        >
          {props.state.busy ? 'Recording…' : 'Retry recording'}
        </Button>
      </View>
    );
  const offered = props.action ? outcomes : [{ value: 'abandoned' as const, label: 'Finish reading' }];
  return (
    <View style={styles.row}>
      {offered.map((outcome) => (
        <Button
          key={outcome.value}
          id={`session-finish:${outcome.value}`}
          disabled={props.state.busy || !props.sitting.session}
          onClick={() => {
            void props.state.finish(outcome.value, props.note, props.confidence);
          }}
        >
          {outcome.label}
        </Button>
      ))}
    </View>
  );
}
function RecordedOutcome(props: { sitting: SittingState; state: FinishState; questions: number }) {
  const session = props.state.result?.session ?? props.sitting.session;
  const minutes = props.state.result?.measured_minutes ?? Math.floor((session?.active_seconds ?? 0) / 60);
  return (
    <View style={styles.column}>
      <Notice tone="success">Recorded: {session?.outcome ?? 'finished'}</Notice>
      <Text style={styles.body}>
        {minutes} measured minute{minutes === 1 ? '' : 's'}. {props.questions} question mark
        {props.questions === 1 ? '' : 's'} saved.
      </Text>
      {session?.note && <Text style={styles.body}>{session.note}</Text>}
      {session?.confidence !== null && session?.confidence !== undefined && (
        <Text style={styles.muted}>Confidence: {session.confidence} / 5</Text>
      )}
      {session?.outcome === 'partial' && minutes === 0 && (
        <Text style={styles.muted}>
          Partial was recorded; with no full measured minute, the plan outcome is deferred.
        </Text>
      )}
      <AppLink href="/study" style={styles.body}>
        Back to plan
      </AppLink>
      <Button
        id="session-restart"
        onClick={() => {
          void props.sitting.restart();
        }}
      >
        Start a new sitting
      </Button>
    </View>
  );
}
