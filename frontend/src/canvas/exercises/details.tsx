import { useState } from 'react';
import type { Exercise } from '@/lib/types';
import { AppLink } from '../app/links';
import { RichText } from '../ui/richText';
import { Text, View } from '../ui/primitives';
import { Dialog } from '../ui/overlays';
import { ExerciseAction } from './controls';
import { exerciseKey } from './model';
import { styles } from './styles';
import { useExerciseDetails } from './useDetails';
import type { MutationState } from './useExercises';

interface DetailProps {
  exercise: Exercise;
  mutation?: MutationState;
  onSave: (ignored: boolean) => void;
}

function ExerciseMetadata({ exercise }: { exercise: Exercise }) {
  return (
    <View style={styles.details}>
      {exercise.description && <RichText html={exercise.description} style={styles.body} />}
      {exercise.work_type && <Text style={styles.body}>Type: {exercise.work_type}</Text>}
      {exercise.assignment_file_url && (
        <AppLink href={exercise.assignment_file_url} target="_blank" rel="noopener" style={styles.attachment}>
          Attachment: {exercise.assignment_file_name || 'Open assignment file'} ↗
        </AppLink>
      )}
      {exercise.grade_comments && <Text style={styles.body}>Feedback: {exercise.grade_comments}</Text>}
      {exercise.submission_date && <Text style={styles.body}>Submitted: {exercise.submission_date}</Text>}
    </View>
  );
}

function HideConfirmation({ id, onCancel, onConfirm }: { id: string; onCancel: () => void; onConfirm: () => void }) {
  return (
    <Dialog
      id={`exercise-confirm-${id}`}
      title="Hide this exercise?"
      open
      onClose={onCancel}
      actions={
        <>
          <ExerciseAction id={`exercise-cancel-hide-${id}`} onClick={onCancel}>
            Cancel
          </ExerciseAction>
          <ExerciseAction id={`exercise-confirm-hide-${id}`} style={styles.hide} onClick={onConfirm}>
            Hide exercise
          </ExerciseAction>
        </>
      }
    >
      <Text style={styles.body}>
        It will leave the active exercise list. You can restore it from the Hidden filter or undo immediately.
      </Text>
    </Dialog>
  );
}

function LoadedExerciseDetails({ exercise, mutation, onSave }: DetailProps) {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const id = exerciseKey(exercise);
  const hidden = Boolean(exercise.ignored);
  const busy = Boolean(mutation?.busy);
  return (
    <View style={styles.details}>
      <ExerciseMetadata exercise={exercise} />
      <ExerciseAction
        id={`exercise-ignore-${id}`}
        disabled={busy}
        style={hidden ? styles.restore : undefined}
        onClick={() => (hidden ? onSave(false) : setConfirmOpen(true))}
      >
        {busy ? `${hidden ? 'Restoring' : 'Hiding'}…` : hidden ? 'Restore exercise' : 'Hide exercise'}
      </ExerciseAction>
      {confirmOpen && !busy && !hidden && (
        <HideConfirmation
          id={id}
          onCancel={() => setConfirmOpen(false)}
          onConfirm={() => {
            setConfirmOpen(false);
            onSave(true);
          }}
        />
      )}
    </View>
  );
}

export function ExerciseDetails(props: DetailProps) {
  const [open, setOpen] = useState(false);
  const id = exerciseKey(props.exercise);
  const { data, error, retry } = useExerciseDetails(props.exercise, open);
  return (
    <View style={styles.details}>
      <ExerciseAction
        id={`exercise-disclosure-${id}`}
        style={styles.disclosure}
        icon={open ? 'chevron-up' : 'chevron-down'}
        onClick={() => setOpen((previous) => !previous)}
      >
        {open ? 'Close assignment details' : 'View assignment details'}
      </ExerciseAction>
      {props.mutation?.error && <Text style={styles.error}>The change was not saved: {props.mutation.error}</Text>}
      {open && error && (
        <View style={styles.details}>
          <Text style={styles.error}>Assignment details could not load: {error}</Text>
          <ExerciseAction id={`exercise-detail-retry-${id}`} onClick={retry}>
            Try again
          </ExerciseAction>
        </View>
      )}
      {open && !error && !data && <Text style={styles.muted}>Loading assignment details…</Text>}
      {open && data && (
        <LoadedExerciseDetails
          {...props}
          exercise={{ ...props.exercise, ...data.exercise, ignored: props.exercise.ignored }}
        />
      )}
    </View>
  );
}
