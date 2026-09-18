import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { z } from 'zod/v4';
import type { Exercise } from '@/lib/types';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import { Icon } from '@/components/Icon';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { ExerciseDetailDisclosure } from './exerciseDetailFields';

const styles = stylex.create({
  exerciseStatusCompleted: {
    color: colors.success,
  },
  exerciseStatusUpcoming: {
    color: colors.info,
  },
  exerciseStatusOverdue: {
    color: colors.danger,
  },
  exerciseRowHeading: {
    gap: '0.75rem',
    display: { default: 'flex', [media.narrow]: 'block' },
    justifyContent: 'space-between',
  },
  exerciseStateLabel: {
    color: colors.textSecondary,
    display: 'block',
    fontSize: '0.625rem',
    textTransform: 'uppercase',
    marginBottom: '0.15rem',
  },
  exerciseTitle: {
    textDecoration: 'none',
    color: { default: colors.textPrimary, ':hover': colors.success },
    display: 'block',
    fontSize: typography.sizeBase,
    lineHeight: 1.3,
  },
  exerciseRowMeta: {
    color: colors.textSecondary,
    columnGap: '0.75rem',
    display: 'flex',
    flexWrap: 'wrap',
    fontSize: typography.sizeXs,
    rowGap: '0.25rem',
    marginTop: '0.35rem',
  },
  exerciseCourse: {
    textDecoration: 'none',
    alignSelf: 'flex-end',
    color: { default: colors.textSecondary, ':hover': colors.textPrimary },
    display: { default: 'inline', [media.narrow]: 'block' },
    flexBasis: 'auto',
    flexGrow: 0,
    flexShrink: 0,
    fontSize: typography.sizeSm,
    marginTop: { default: null, [media.narrow]: '0.45rem' },
  },
  exerciseRow: {
    borderColor: { default: colors.border, ':focus-within': colors.borderLight },
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.625rem',
    paddingBlock: '0.7rem',
    paddingInline: '0.85rem',
    alignItems: 'start',
    backgroundColor: colors.surface,
    display: 'grid',
    gridTemplateColumns: {
      default: '2rem minmax(0, 1fr)',
      [media.narrow]: '1.8rem minmax(0, 1fr)',
    },
  },
  exerciseIgnored: {
    opacity: 0.72,
  },
  exerciseUrgency: {
    color: colors.danger,
    fontSize: typography.sizeSm,
    whiteSpace: 'nowrap',
  },
  exerciseUrgencyQuiet: {
    color: colors.textSecondary,
    fontWeight: typography.weightNormal,
  },
  exerciseRowError: {
    color: colors.danger,
    fontSize: typography.sizeSm,
  },
  exerciseStatusMark: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    boxSizing: 'border-box',
    display: 'flex',
    fontSize: '1rem',
    justifyContent: 'center',
    height: '2rem',
    minHeight: '2rem',
  },
  exerciseRowMain: {
    minWidth: 0,
  },
  exerciseGrade: {
    alignItems: 'center',
    alignSelf: 'flex-start',
    display: 'inline-flex',
  },
  externalIcon: {
    color: colors.textSecondary,
    display: 'inline-block',
    fontSize: '0.75em',
    verticalAlign: 'text-bottom',
    marginLeft: '0.25rem',
  },
  awardIcon: {
    marginRight: '0.2rem',
  },
});

const errorBodySchema = z
  .object({
    detail: z.unknown(),
  })
  .partial();

const detailItemSchema = z
  .object({
    msg: z.string(),
  })
  .partial();

const EXERCISE_STATUS = {
  completed: { icon: 'check-lg', style: styles.exerciseStatusCompleted },
  upcoming: { icon: 'clock', style: styles.exerciseStatusUpcoming },
  overdue: {
    icon: 'exclamation-lg',
    style: styles.exerciseStatusOverdue,
  },
} as const;

type ExerciseStatus = keyof typeof EXERCISE_STATUS;

function exerciseStatus(exercise: Exercise): ExerciseStatus {
  return exercise.submission_status === 'submitted' ? 'completed' : 'upcoming';
}

async function postExerciseAction(
  path: string,
  setBusy: (value: boolean) => void,
  setError: (value: string | null) => void,
): Promise<boolean> {
  setBusy(true);
  setError(null);
  try {
    const response = await fetch(path, {
      method: 'POST',
      headers: { Accept: 'application/json' },
    });
    if (!response.ok) {
      const parsed = errorBodySchema.safeParse(await response.json().catch(() => ({})));
      const detail = parsed.success ? parsed.data.detail : undefined;
      throw new Error(
        Array.isArray(detail)
          ? detail
              .map((item) => {
                const parsedItem = detailItemSchema.safeParse(item);
                return parsedItem.success && parsedItem.data.msg ? parsedItem.data.msg : String(item);
              })
              .join('. ')
          : String(detail || `The change was not saved (HTTP ${response.status}).`),
      );
    }
    return true;
  } catch (postError) {
    setError(postError instanceof Error ? postError.message : 'Could not save the change.');
    return false;
  } finally {
    setBusy(false);
  }
}

export function useIgnoreAction(
  exercise: Exercise,
  onIgnore: (courseId: string | number, exerciseId: string | number) => void,
  onRestore: (courseId: string | number, exerciseId: string | number) => void,
) {
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const post = (path: string): Promise<boolean> => postExerciseAction(path, setBusy, setError);
  const ignore = async () => {
    if (await post(`/exercises/${exercise.course_id}/${encodeURIComponent(String(exercise.exercise_id))}/ignore`))
      onIgnore(exercise.course_id, exercise.exercise_id);
  };
  const restore = async () => {
    if (await post(`/exercises/${exercise.course_id}/${encodeURIComponent(String(exercise.exercise_id))}/unignore`))
      onRestore(exercise.course_id, exercise.exercise_id);
  };
  const action = exercise.ignored ? restore : ignore;
  const actionLabel = exercise.ignored ? 'Restore exercise' : 'Hide exercise';
  return { busy, error, action, actionLabel };
}

function deadlineLabel(exercise: Exercise): string {
  if (exercise._time_label) return exercise._time_label;
  const short = String(exercise.deadline_short || '').replace(' · ', ', ');
  if (short) return exercise.submission_status === 'submitted' ? `Due ${short}` : short;
  return exercise.deadline || 'No deadline';
}

export function ExerciseRowHeading({ exercise }: { exercise: Exercise }) {
  return (
    <div {...stylex.props(styles.exerciseRowHeading)}>
      <ExerciseRowTitle exercise={exercise} />
      <strong
        {...stylex.props(
          styles.exerciseUrgency,
          exercise.submission_status === 'submitted' && styles.exerciseUrgencyQuiet,
        )}
      >
        {deadlineLabel(exercise)}
      </strong>
    </div>
  );
}

function ExerciseRowTitle({ exercise }: { exercise: Exercise }) {
  return (
    <div>
      <span {...stylex.props(styles.exerciseStateLabel)}>{exercise.submission_status || 'Upcoming'}</span>
      <a
        {...stylex.props(styles.exerciseTitle)}
        href={exercise.link || '#'}
        target="_blank"
        rel="noopener"
        aria-label={`Open external assignment: ${exercise.title}`}
      >
        {exercise.title}{' '}
        <span {...stylex.props(styles.externalIcon)}>
          <Icon name="box-arrow-up-right" aria-hidden="true" />
        </span>
      </a>
    </div>
  );
}

export function ExerciseRowMeta({ exercise }: { exercise: Exercise }) {
  return (
    <div {...stylex.props(styles.exerciseRowMeta)}>
      <span {...stylex.props(styles.exerciseCourse)}>{exercise.course_name}</span>
      {exercise.grade && (
        <span {...stylex.props(styles.exerciseGrade)}>
          <span {...stylex.props(styles.awardIcon)}>
            <Icon name="award" aria-hidden="true" />
          </span>{' '}
          Grade {exercise.grade}
        </span>
      )}
    </div>
  );
}

function ExerciseDetailBody({
  exercise,
  busy,
  error,
  action,
  actionLabel,
  onRequestHide,
}: {
  exercise: Exercise;
  busy: boolean;
  error: string | null;
  action: () => Promise<void>;
  actionLabel: string;
  onRequestHide: () => void;
}) {
  return (
    <>
      {error && (
        <p {...stylex.props(styles.exerciseRowError)} role="alert">
          {error}
        </p>
      )}
      <ExerciseDetailDisclosure
        exercise={exercise}
        busy={busy}
        action={action}
        actionLabel={actionLabel}
        onRequestHide={onRequestHide}
      />
    </>
  );
}

export function ExerciseDetails({
  exercise,
  busy,
  error,
  action,
  actionLabel,
}: {
  exercise: Exercise;
  busy: boolean;
  error: string | null;
  action: () => Promise<void>;
  actionLabel: string;
}) {
  const [confirmOpen, setConfirmOpen] = React.useState(false);
  return (
    <>
      <ExerciseDetailBody
        exercise={exercise}
        busy={busy}
        error={error}
        action={action}
        actionLabel={actionLabel}
        onRequestHide={() => setConfirmOpen(true)}
      />
      <ConfirmDialog
        open={confirmOpen}
        title="Hide this exercise?"
        description={
          'It will leave the active exercise list. You can restore it from the Hidden filter ' + 'or undo immediately.'
        }
        confirmLabel="Hide exercise"
        danger
        onCancel={() => setConfirmOpen(false)}
        onConfirm={() => {
          setConfirmOpen(false);
          action();
        }}
      />
    </>
  );
}

export function ExerciseRow({
  exercise,
  onIgnore,
  onRestore,
}: {
  exercise: Exercise;
  onIgnore: (courseId: string | number, exerciseId: string | number) => void;
  onRestore: (courseId: string | number, exerciseId: string | number) => void;
}) {
  const status = exerciseStatus(exercise);
  const { style: statusStyle, icon: statusIcon } = EXERCISE_STATUS[status];
  const { busy, error, action, actionLabel } = useIgnoreAction(exercise, onIgnore, onRestore);
  return (
    <article
      {...stylex.props(styles.exerciseRow, exercise.ignored && styles.exerciseIgnored)}
      data-status={status}
      data-urgency={exercise._urgency}
      data-ignored={exercise.ignored || undefined}
    >
      <div {...stylex.props(styles.exerciseStatusMark, statusStyle)} aria-hidden="true">
        <Icon name={statusIcon} aria-hidden="true" />
      </div>
      <div {...stylex.props(styles.exerciseRowMain)}>
        <ExerciseRowHeading exercise={exercise} />
        <ExerciseRowMeta exercise={exercise} />
        <ExerciseDetails exercise={exercise} busy={busy} error={error} action={action} actionLabel={actionLabel} />
      </div>
    </article>
  );
}
