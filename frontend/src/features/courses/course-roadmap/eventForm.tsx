import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { Check, ChevronDown, CirclePause, CircleX, Clock3, Play, Undo2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Popover } from '@astryxdesign/core/Popover';
import { Textarea } from '@/components/ui/textarea';
import { fetchJson } from '@/lib/api';
import { requestKey } from '@/features/session/reader/api';
import type { RoadmapAction, CourseDetailPayload } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  courseActionForm: {
    gap: '.5rem',
    display: 'flex',
    flexDirection: 'column',
    marginTop: '.5rem',
  },
  courseActionControls: {
    gap: '.4rem',
    display: 'flex',
  },
  courseOutcomePopover: {
    padding: '.75rem',
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '.5rem',
    backgroundColor: colors.surface,
    display: 'flex',
    flexDirection: 'column',
    width: 'min(22rem, calc(100vw - 2rem))',
  },
  courseOutcomeOptions: {
    gap: '.25rem',
    display: 'grid',
    gridTemplateColumns: 'repeat(2, 1fr)',
  },
  courseOutcomeButton: {
    font: 'inherit',
    padding: '.55rem',
    borderRadius: '.35rem',
    borderStyle: 'none',
    borderWidth: 0,
    gap: '.45rem',
    alignItems: 'center',
    backgroundColor: {
      default: 'transparent',
      ':hover': colors.surfaceHover,
    },
    color: colors.textPrimary,
    cursor: 'pointer',
    display: 'flex',
    fontSize: '.75rem',
    textAlign: 'left',
  },
  courseActionReflection: {
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    marginTop: '.35rem',
  },
  courseActionReflectionSummary: {
    gap: '.4rem',
    paddingInline: '.25rem',
    alignItems: 'center',
    color: colors.textSecondary,
    cursor: 'pointer',
    display: 'flex',
    fontSize: typography.sizeXs,
    paddingBlockEnd: 0,
    paddingBlockStart: '.55rem',
  },
  courseActionReflectionBody: {
    gap: '.65rem',
    display: 'grid',
    marginTop: '.6rem',
  },
  courseActionFieldLabel: {
    gap: '.25rem',
    color: colors.textSecondary,
    display: 'grid',
    fontSize: typography.sizeXs,
  },
  courseConfidenceField: {
    gap: '.25rem',
    display: 'flex',
  },
  courseActionError: {
    marginInline: 0,
    color: colors.danger,
    fontSize: typography.sizeXs,
    marginBlockEnd: 0,
    marginBlockStart: '.4rem',
  },
  courseActionStatusMsg: {
    marginInline: 0,
    color: colors.success,
    fontSize: typography.sizeXs,
    marginBlockEnd: 0,
    marginBlockStart: '.4rem',
  },
});

const OUTCOMES = [
  { event: 'completed', label: 'Completed', icon: Check },
  { event: 'partial', label: 'Partly done', icon: CirclePause },
  { event: 'stuck', label: 'Stuck', icon: CircleX },
  { event: 'deferred', label: 'Do later', icon: Undo2 },
];

interface EventBody {
  course_id: string | number;
  event_type: string;
  action_id?: string;
  plan_revision: string;
  actual_minutes?: number;
  confidence?: number;
  note?: string;
  idempotency_key?: string;
}

function buildEventBody(
  action: RoadmapAction,
  courseId: string | number,
  revision: string | undefined,
  eventType: string,
  form: HTMLFormElement,
): { body: EventBody } | { error: string } {
  const data = new FormData(form);
  const body: EventBody = {
    course_id: courseId,
    event_type: eventType,
    action_id: action.action_id || String(action.id || ''),
    plan_revision: revision || '',
  };
  const minutes = Number(data.get('actual_minutes'));
  if (Number.isInteger(minutes) && minutes > 0) body.actual_minutes = minutes;
  if (eventType === 'partial' && !body.actual_minutes) return { error: 'Add minutes first.' };
  const confidence = data.get('confidence');
  if (confidence !== '') body.confidence = Number(confidence);
  const note = String(data.get('note') || '').trim();
  if (note) body.note = note;
  return { body };
}

interface RecordEventArgs {
  action: RoadmapAction;
  courseId: string | number;
  revision?: string;
  eventType: string;
  form: HTMLFormElement;
  keyMap: Map<string, string>;
  setBusy: (value: boolean) => void;
  setError: (value: string | null) => void;
  setStatus: (value: string | null) => void;
  onStatus?: (status: string) => void;
}

async function recordEvent(args: RecordEventArgs): Promise<void> {
  const { action, courseId, revision, eventType, form, keyMap } = args;
  args.setBusy(true);
  args.setError(null);
  args.setStatus(null);
  const result = buildEventBody(action, courseId, revision, eventType, form);
  if ('error' in result) {
    args.setError(result.error);
    args.setBusy(false);
    return;
  }
  const key = keyMap.get(eventType) || requestKey('roadmap');
  keyMap.set(eventType, key);
  try {
    const saved = await fetchJson<{ overview: CourseDetailPayload }>('api/v1/study/actions/event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...result.body, idempotency_key: key }),
    });
    window.dispatchEvent(new CustomEvent('tree:course-progress', { detail: saved.overview }));
    keyMap.delete(eventType);
    args.setStatus(`Recorded: ${eventType}`);
    args.onStatus?.(eventType);
  } catch (error) {
    args.setError(error instanceof Error ? error.message : 'Could not record progress');
  } finally {
    args.setBusy(false);
  }
}

function ReflectionFields({ action, formId }: { action: RoadmapAction; formId: string }) {
  const [confidence, setConfidence] = React.useState('');
  return (
    <details {...stylex.props(styles.courseActionReflection)}>
      <summary {...stylex.props(styles.courseActionReflectionSummary)}>
        <Clock3 size={13} /> Add details
      </summary>
      <div {...stylex.props(styles.courseActionReflectionBody)}>
        <label {...stylex.props(styles.courseActionFieldLabel)}>
          <span>Minutes</span>
          <Input
            type="number"
            name="actual_minutes"
            form={formId}
            min="1"
            max="1440"
            defaultValue={action.remaining_minutes || action.estimated_minutes || 15}
          />
        </label>
        <label {...stylex.props(styles.courseActionFieldLabel)}>
          <span>Confidence</span>
          <input type="hidden" name="confidence" value={confidence} form={formId} />
          <div {...stylex.props(styles.courseConfidenceField)}>
            {['1', '3', '5'].map((value) => (
              <Button
                type="button"
                size="sm"
                variant={confidence === value ? 'secondary' : 'ghost'}
                onClick={() => setConfidence(value)}
                key={value}
              >
                {value}
              </Button>
            ))}
          </div>
        </label>
        <label {...stylex.props(styles.courseActionFieldLabel)}>
          <span>Note</span>
          <Textarea name="note" form={formId} maxLength={4000} placeholder="What clicked or blocked you?" />
        </label>
      </div>
    </details>
  );
}

function FinishActionPopover({ action, formId, busy }: { action: RoadmapAction; formId: string; busy: boolean }) {
  return (
    <Popover
      placement="below"
      alignment="start"
      content={
        <div {...stylex.props(styles.courseOutcomePopover)}>
          <div {...stylex.props(styles.courseOutcomeOptions)}>
            {OUTCOMES.map(({ event, label, icon: Icon }) => (
              <button
                type="submit"
                name="event_type"
                value={event}
                form={formId}
                disabled={busy}
                key={event}
                {...stylex.props(styles.courseOutcomeButton)}
              >
                <Icon size={14} /> {label}
              </button>
            ))}
          </div>
          <ReflectionFields action={action} formId={formId} />
        </div>
      }
    >
      <Button type="button" size="sm" variant="outline" endContent={<ChevronDown aria-hidden="true" />}>
        Finish
      </Button>
    </Popover>
  );
}

function ActionControls({
  action,
  courseId,
  formId,
  busy,
}: {
  action: RoadmapAction;
  courseId: string | number;
  formId: string;
  busy: boolean;
}) {
  const actionId = encodeURIComponent(action.action_id || String(action.id || ''));
  return (
    <div {...stylex.props(styles.courseActionControls)}>
      <Button
        size="sm"
        href={`/study/session?course_id=${courseId}&action_id=${actionId}`}
        icon={<Play fill="currentColor" aria-hidden="true" />}
      >
        Start
      </Button>
      <FinishActionPopover action={action} formId={formId} busy={busy} />
    </div>
  );
}

interface EventFormProps {
  action: RoadmapAction;
  courseId: string | number;
  revision?: string;
  onStatus?: (status: string) => void;
}

function useEventForm({ action, courseId, revision, onStatus }: EventFormProps) {
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [status, setStatus] = React.useState<string | null>(null);
  const formId = React.useId();
  const keyMap = React.useRef(new Map<string, string>());
  const submit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    // SAFETY: React wraps the browser's SubmitEvent for form submissions.
    const nativeEvent = event.nativeEvent as SubmitEvent;
    // SAFETY: every submitter in this form is an outcome button.
    const submitter = nativeEvent.submitter as HTMLButtonElement | null;
    if (!submitter?.value) return;
    recordEvent({
      action,
      courseId,
      revision,
      eventType: submitter.value,
      form: event.currentTarget,
      keyMap: keyMap.current,
      setBusy,
      setError,
      setStatus,
      onStatus,
    });
  };
  return { busy, error, status, formId, submit };
}

export function EventForm(props: EventFormProps) {
  const { busy, error, status, formId, submit } = useEventForm(props);
  return (
    <form id={formId} {...stylex.props(styles.courseActionForm)} onSubmit={submit}>
      <ActionControls action={props.action} courseId={props.courseId} formId={formId} busy={busy} />
      {error && (
        <p role="alert" {...stylex.props(styles.courseActionError)}>
          {error}
        </p>
      )}
      {status && (
        <p role="status" {...stylex.props(styles.courseActionStatusMsg)}>
          {status}
        </p>
      )}
    </form>
  );
}
