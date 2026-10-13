import { AppLink } from '../app/links';
import { Button, Notice } from '../ui/controls';
import { Field } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';
import type { RecallOutcome } from './types';
import type { RecallState } from './useRecall';

const outcomes: { value: RecallOutcome; label: string }[] = [
  { value: 'correct', label: 'Got it' },
  { value: 'partial', label: 'Partly' },
  { value: 'incorrect', label: 'Not yet' },
  { value: 'skipped', label: 'Skip' },
];
function RecallEmptyQueue({ course, state }: { course: string; state: RecallState }) {
  return (
    <View style={styles.column}>
      <Text style={styles.title}>{state.practice?.enabled ? 'Nothing due' : 'No practice questions yet'}</Text>
      {!state.practice?.enabled && (
        <Text style={styles.muted}>
          Practice generation requires KNOWLEDGE_AI_PRACTICE_ENABLED and an enabled course roadmap.
        </Text>
      )}
      <AppLink href={`/courses/${encodeURIComponent(course)}#roadmap`} style={styles.body}>
        Course roadmap
      </AppLink>
    </View>
  );
}
export function RecallPanel({ course, state }: { course: string; state: RecallState }) {
  if (state.loading && !state.practice) return <Text style={styles.muted}>Loading recall…</Text>;
  if (!state.practice)
    return (
      <View style={styles.column}>
        {state.error && <Notice tone="danger">{state.error}</Notice>}
        <Button id={`recall:${course}:read-retry`} disabled={state.loading} onClick={state.refresh}>
          Try again
        </Button>
      </View>
    );
  if (!state.question) return <RecallEmptyQueue course={course} state={state} />;
  const id = `recall:${encodeURIComponent(
    JSON.stringify([course, state.question.unit_key, state.question.question_id ?? state.question.id]),
  )}`;
  const locked = state.busy || Boolean(state.pending) || state.saved?.practice === null;
  return (
    <View style={styles.column}>
      <Text style={styles.title}>{state.question.unit_title || state.question.unit_key || 'Recall'}</Text>
      <Text style={styles.muted}>
        {state.count} question{state.count === 1 ? '' : 's'} waiting
      </Text>
      <Text style={styles.body}>{state.question.prompt ?? ''}</Text>
      <Field
        id={`${id}:answer`}
        label="Your recall answer"
        value={state.answer}
        onChange={state.setAnswer}
        multiline
        rows={4}
        disabled={state.busy}
        readOnly={locked}
        placeholder="Answer from memory first…"
      />
      {state.revealed ? (
        <RecallAnswer id={id} state={state} locked={locked} />
      ) : (
        <Button id={`${id}:reveal`} onClick={state.reveal}>
          Show the answer
        </Button>
      )}
      <RecallStatus id={id} state={state} />
    </View>
  );
}
function RecallAnswer({ id, state, locked }: { id: string; state: RecallState; locked: boolean }) {
  return (
    <View style={styles.column}>
      <Text style={styles.body}>{state.question?.expected_answer ?? ''}</Text>
      {state.question?.answer_points?.map((point, index) => (
        <Text key={`${id}:point:${index}:${point}`} style={styles.body}>
          • {point}
        </Text>
      ))}
      <View style={styles.row}>
        {outcomes.map((outcome) => (
          <Button
            key={outcome.value}
            id={`${id}:${outcome.value}`}
            disabled={locked}
            onClick={() => {
              void state.record(outcome.value);
            }}
          >
            {outcome.label}
          </Button>
        ))}
      </View>
    </View>
  );
}
function RecallStatus({ id, state }: { id: string; state: RecallState }) {
  return (
    <View style={styles.column}>
      {state.busy && <Text style={styles.muted}>Recording attempt…</Text>}
      {state.error && <Notice tone="danger">{state.error}</Notice>}
      {state.pending && !state.busy && (
        <Button
          id={`${id}:save-retry`}
          onClick={() => {
            void state.retrySave();
          }}
        >
          Retry the same attempt
        </Button>
      )}
      {state.saved?.practice === null && (
        <>
          <Notice tone="warning">
            Attempt recorded. The practice queue refresh is pending.
            {state.saved.practice_error ? ` ${state.saved.practice_error}` : ''} Do not submit this answer again.
          </Notice>
          <Button id={`${id}:refresh`} disabled={state.loading} onClick={state.refresh}>
            {state.loading ? 'Refreshing…' : 'Refresh practice queue'}
          </Button>
        </>
      )}
      {state.saved?.practice && <Notice tone="success">Attempt recorded.</Notice>}
    </View>
  );
}
