import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { Check, ChevronDown, CirclePause, CircleX, Clock3, Undo2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Popover } from '@astryxdesign/core/Popover';
import { Input } from '@/components/ui/input';
import { fetchJson } from '@/lib/api';
import { requestKey } from '@/features/session/reader/api';
import type { StudyAction } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  studyPrimaryActions: {
    margin: 0,
    gap: '0.5rem',
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
  },
  studyStartButton: {
    fontWeight: typography.weightBold,
    minWidth: '9rem',
  },
  studyPlayIcon: {
    fontSize: '0.65rem',
    marginRight: '0.35rem',
  },
  studyOutcomePopover: {
    padding: '0.45rem',
    gap: '0.25rem',
    display: 'flex',
    flexDirection: 'column',
    width: 'min(22rem, calc(100vw - 2rem))',
  },
  studyOutcomeOptions: {
    gap: '0.25rem',
    display: 'grid',
    gridTemplateColumns: '1fr 1fr',
  },
  studyOutcomeOption: {
    font: 'inherit',
    borderRadius: layout.radiusMedium,
    borderWidth: 0,
    gap: '0.5rem',
    paddingBlock: '0.55rem',
    paddingInline: '0.65rem',
    alignItems: 'center',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceHover },
    color: colors.textPrimarySoft,
    cursor: 'pointer',
    display: 'flex',
    fontSize: typography.sizeSm,
    justifyContent: 'flex-start',
    textAlign: 'left',
    minHeight: '2.5rem',
  },
  studyOutcomeOptionIcon: {
    height: '1rem',
    width: '1rem',
  },
  studyReflection: {
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    marginTop: '0.35rem',
    paddingTop: '0.45rem',
  },
  studyReflectionSummary: {
    gap: '0.45rem',
    listStyle: 'none',
    paddingBlock: '0.5rem',
    paddingInline: '0.65rem',
    alignItems: 'center',
    color: colors.textSecondary,
    cursor: 'pointer',
    display: 'flex',
    fontSize: typography.sizeSm,
  },
  studyEventFields: {
    padding: '0.45rem',
    gap: '0.55rem',
    display: 'grid',
    gridTemplateColumns: '1fr 1fr',
  },
  studyEventLabel: {
    gap: '0.35rem',
    color: colors.textSecondary,
    display: 'grid',
    fontSize: typography.sizeXs,
  },
  studyEventSelect: {
    borderColor: colors.border,
    borderRadius: layout.radiusMedium,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingInline: '0.75rem',
    backgroundColor: colors.surface,
    color: colors.textPrimary,
    height: '2.5rem',
    width: '100%',
  },
  studyEventNote: {
    gridColumnEnd: '-1',
    gridColumnStart: '1',
  },
  studyEventStatus: {
    marginInline: 0,
    fontSize: typography.sizeSm,
    marginBlockEnd: 0,
    marginBlockStart: '0.75rem',
  },
  statusError: {
    color: colors.statusDanger,
  },
  statusSuccess: {
    color: colors.statusSuccess,
  },
  studyOutcomeForm: {
    display: 'flex',
    flexDirection: 'column',
  },
});

const OUTCOMES = [
  { event: 'completed', label: 'Completed', icon: Check },
  { event: 'partial', label: 'Partly done', icon: CirclePause },
  { event: 'stuck', label: 'Stuck', icon: CircleX },
  { event: 'deferred', label: 'Do later', icon: Undo2 },
];

interface OutcomeBody {
  course_id: string | number;
  event_type: string;
  action_id?: string;
  plan_revision: string;
  actual_minutes?: number;
  confidence?: number;
  note?: string;
  idempotency_key?: string;
}

function buildOutcomeBody(
  action: StudyAction,
  eventType: string,
  form: HTMLFormElement,
): { body: OutcomeBody } | { error: string } {
  const data = new FormData(form);
  const body: OutcomeBody = {
    course_id: action.course_id,
    event_type: eventType,
    action_id: action.action_id,
    plan_revision: action.blueprint_revision_id || action.plan_revision || '',
  };
  const minutes = Number(data.get('actual_minutes'));
  if (Number.isInteger(minutes) && minutes > 0) body.actual_minutes = minutes;
  if (eventType === 'partial' && !body.actual_minutes) {
    return { error: 'Partial progress requires a positive minutes value.' };
  }
  const confidence = data.get('confidence');
  if (confidence !== '') body.confidence = Number(confidence);
  const note = String(data.get('note') || '').trim();
  if (note) body.note = note;
  return { body };
}

interface RecordOutcomeArgs {
  action: StudyAction;
  eventType: string;
  form: HTMLFormElement;
  idempotencyKeys: React.MutableRefObject<Map<string, string>>;
  setBusy: (busy: boolean) => void;
  setError: (error: string | null) => void;
  setStatus: (status: string | null) => void;
  onRecorded: (eventType: string) => void;
}

async function recordOutcome({
  action,
  eventType,
  form,
  idempotencyKeys,
  setBusy,
  setError,
  setStatus,
  onRecorded,
}: RecordOutcomeArgs): Promise<void> {
  setBusy(true);
  setError(null);
  setStatus(null);
  const result = buildOutcomeBody(action, eventType, form);
  if ('error' in result) {
    setError(result.error);
    setBusy(false);
    return;
  }
  const body = result.body;
  const key = idempotencyKeys.current.get(eventType) || requestKey('study');
  idempotencyKeys.current.set(eventType, key);
  body.idempotency_key = key;
  try {
    await fetchJson('api/v1/study/actions/event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    idempotencyKeys.current.delete(eventType);
    setStatus(`Recorded: ${eventType}`);
    onRecorded(eventType);
  } catch (recordError) {
    setError(recordError instanceof Error ? recordError.message : 'Could not record progress');
  } finally {
    setBusy(false);
  }
}

export function OutcomePrimary({ action, busy, formId }: { action: StudyAction; busy: boolean; formId: string }) {
  return (
    <div {...stylex.props(styles.studyPrimaryActions)}>
      <Button
        size="lg"
        href={`/study/session?course_id=${action.course_id}&action_id=${encodeURIComponent(action.action_id || '')}`}
        {...stylex.props(styles.studyStartButton)}
      >
        <span {...stylex.props(styles.studyPlayIcon)} aria-hidden="true">
          ▶
        </span>
        Start session
      </Button>
      <OutcomeFinish action={action} busy={busy} formId={formId} />
    </div>
  );
}

function OutcomeFinish({ action, busy, formId }: { action: StudyAction; busy: boolean; formId: string }) {
  return (
    <Popover
      placement="below"
      alignment="start"
      content={
        <div {...stylex.props(styles.studyOutcomePopover)}>
          <div {...stylex.props(styles.studyOutcomeOptions)} role="group" aria-label="Log study progress">
            {OUTCOMES.map(({ event, label, icon: Icon }) => (
              <button
                type="submit"
                name="event_type"
                value={event}
                key={event}
                form={formId}
                disabled={busy}
                {...stylex.props(styles.studyOutcomeOption)}
              >
                <Icon aria-hidden="true" {...stylex.props(styles.studyOutcomeOptionIcon)} /> {busy ? 'Saving…' : label}
              </button>
            ))}
          </div>
          <ReflectionFields action={action} formId={formId} />
        </div>
      }
    >
      <Button type="button" variant="outline" size="lg" endContent={<ChevronDown aria-hidden="true" />}>
        Finish
      </Button>
    </Popover>
  );
}

export function ReflectionFields({ action, formId }: { action: StudyAction; formId: string }) {
  return (
    <details {...stylex.props(styles.studyReflection)}>
      <summary {...stylex.props(styles.studyReflectionSummary)}>
        <Clock3 aria-hidden="true" /> Add details
      </summary>
      <div {...stylex.props(styles.studyEventFields)}>
        <label {...stylex.props(styles.studyEventLabel)}>
          <span>Minutes</span>
          <Input
            type="number"
            name="actual_minutes"
            form={formId}
            min="1"
            max="1440"
            defaultValue={action.minutes || action.remaining_minutes || 15}
            inputMode="numeric"
          />
        </label>
        <label {...stylex.props(styles.studyEventLabel)}>
          <span>Confidence</span>
          <select name="confidence" form={formId} defaultValue="" {...stylex.props(styles.studyEventSelect)}>
            <option value="">Not set</option>
            <option value="1">Low</option>
            <option value="3">Fair</option>
            <option value="5">Strong</option>
          </select>
        </label>
        <label {...stylex.props(styles.studyEventLabel, styles.studyEventNote)}>
          <span>Note</span>
          <Input
            type="text"
            name="note"
            form={formId}
            maxLength={4000}
            placeholder="What clicked, or what blocked you?"
          />
        </label>
      </div>
    </details>
  );
}

export function OutcomeStatus({ error, status }: { error: string | null; status: string | null }) {
  return (
    <>
      {error && (
        <p {...stylex.props(styles.studyEventStatus, styles.statusError)} data-state="error" role="alert">
          {error}
        </p>
      )}
      {status && (
        <p {...stylex.props(styles.studyEventStatus, styles.statusSuccess)} data-state="success" role="status">
          {status}
        </p>
      )}
    </>
  );
}

export function RecordOutcome({
  action,
  onRecorded,
}: {
  action: StudyAction;
  onRecorded: (eventType: string) => void;
}) {
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [status, setStatus] = React.useState<string | null>(null);
  const formId = React.useId();
  const idempotencyKeys = React.useRef(new Map<string, string>());
  const submit = React.useCallback(
    (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      // SAFETY: React form events wrap native SubmitEvents; the cast
      // narrows the synthetic event to read the native submitter.
      const nativeEvent = event.nativeEvent as SubmitEvent;
      // SAFETY: the form's submit buttons are the only submitters; the
      // cast narrows the generic submitter to read name/value.
      const submitter = nativeEvent.submitter as HTMLButtonElement | null;
      if (submitter?.name === 'event_type') {
        recordOutcome({
          action,
          eventType: submitter.value,
          form: event.currentTarget,
          idempotencyKeys,
          setBusy,
          setError,
          setStatus,
          onRecorded,
        });
      }
    },
    [action, idempotencyKeys, onRecorded],
  );
  return (
    <form id={formId} {...stylex.props(styles.studyOutcomeForm)} onSubmit={submit}>
      <OutcomePrimary action={action} busy={busy} formId={formId} />
      <OutcomeStatus error={error} status={status} />
    </form>
  );
}
