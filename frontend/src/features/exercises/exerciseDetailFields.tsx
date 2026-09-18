import { useDeferredResource } from '@/lib/useDeferredResource';
import { Button } from '@/components/ui/button';
import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import type { Exercise } from '@/lib/types';
import { colors, typography } from '@/styles/tokens.stylex';
import { plainExerciseText, safeExerciseHtml } from './exerciseHtml';

const styles = stylex.create({
  exerciseDescription: {
    color: colors.textPrimarySoft,
    fontSize: typography.sizeBase,
    lineHeight: 1.55,
    overflowWrap: 'anywhere',
    paddingTop: '0.75rem',
  },
  exerciseDetailsBody: {
    gap: '0.65rem',
    color: colors.textSecondary,
    display: 'grid',
    paddingBottom: 0,
    paddingTop: '0.75rem',
  },
  exerciseRestore: {
    borderColor: colors.success,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surfaceSuccess,
    color: colors.success,
  },
  exerciseDetails: {},
  summary: {
    gap: '0.5rem',
    listStyle: 'none',
    alignItems: 'center',
    color: colors.textSecondary,
    cursor: 'pointer',
    display: 'flex',
    fontSize: typography.sizeSm,
    fontWeight: typography.weightSemibold,
    justifyContent: 'flex-start',
    paddingTop: '0.7rem',
  },
  m0: { margin: 0 },
  chevron: {
    display: 'inline-flex',
    transitionDuration: '150ms',
    transitionProperty: 'transform',
  },
});

export interface ExerciseDetailDisclosureProps {
  exercise: Exercise;
  busy: boolean;
  action: () => Promise<void>;
  actionLabel: string;
  onRequestHide: () => void;
}

function ExerciseDescription({ value }: { value: string }) {
  const [htmlReady, setHtmlReady] = React.useState(false);
  React.useEffect(() => setHtmlReady(true), []);
  return (
    <div
      {...stylex.props(styles.exerciseDescription)}
      dangerouslySetInnerHTML={{
        __html: htmlReady ? safeExerciseHtml(value) : plainExerciseText(value),
      }}
    />
  );
}

function ExerciseDetailFields({ exercise, busy, action, actionLabel, onRequestHide }: ExerciseDetailDisclosureProps) {
  const isHidden = Boolean(exercise.ignored);
  return (
    <div {...stylex.props(styles.exerciseDetailsBody)}>
      <ExerciseDetailMetadata exercise={exercise} />
      <button
        {...stylex.props(buttonStyles.base, isHidden ? styles.exerciseRestore : buttonStyles.secondary)}
        disabled={busy}
        onClick={() => (isHidden ? action() : onRequestHide())}
      >
        {busy ? `${isHidden ? 'Restoring' : 'Hiding'}…` : actionLabel}
      </button>
    </div>
  );
}

function ExerciseDetailMetadata({ exercise }: { exercise: Exercise }) {
  return (
    <>
      {exercise.description && <ExerciseDescription value={exercise.description} />}
      {exercise.work_type && (
        <p {...stylex.props(styles.m0)}>
          <strong>Type:</strong> {exercise.work_type}
        </p>
      )}
      {exercise.assignment_file_url && (
        <a href={exercise.assignment_file_url} target="_blank" rel="noopener">
          <Icon name="paperclip" aria-hidden="true" /> {exercise.assignment_file_name || 'Open assignment file'}{' '}
          <Icon name="box-arrow-up-right" aria-hidden="true" />
        </a>
      )}
      {exercise.grade_comments && (
        <p {...stylex.props(styles.m0)}>
          <strong>Feedback:</strong> {exercise.grade_comments}
        </p>
      )}
      {exercise.submission_date && (
        <p {...stylex.props(styles.m0)}>
          <strong>Submitted:</strong> {exercise.submission_date}
        </p>
      )}
    </>
  );
}

function DeferredExerciseFields(props: ExerciseDetailDisclosureProps & { open: boolean }) {
  const { exercise, open } = props;
  const path = `/api/v1/courses/${exercise.course_id}/exercises/${encodeURIComponent(String(exercise.exercise_id))}`;
  const { data, error, retry } = useDeferredResource<{ exercise: Exercise }>(path, open);
  if (!open) return null;
  if (error)
    return (
      <p role="alert">
        Assignment details could not load. <Button onClick={retry}>Try again</Button>
      </p>
    );
  if (!data) return <p role="status">Loading assignment details…</p>;
  return <ExerciseDetailFields {...props} exercise={{ ...exercise, ...data.exercise, ignored: exercise.ignored }} />;
}

export function ExerciseDetailDisclosure(props: ExerciseDetailDisclosureProps) {
  const [open, setOpen] = React.useState(false);
  return (
    <details
      open={open}
      onToggle={(event) => setOpen(event.currentTarget.open)}
      {...stylex.props(styles.exerciseDetails)}
    >
      <summary aria-label={`View assignment details for ${props.exercise.title}`} {...stylex.props(styles.summary)}>
        View assignment details{' '}
        <span {...stylex.props(styles.chevron)}>
          <Icon name="chevron-down" aria-hidden="true" />
        </span>
      </summary>
      <DeferredExerciseFields key={props.exercise.fetched_at} {...props} open={open} />
    </details>
  );
}
