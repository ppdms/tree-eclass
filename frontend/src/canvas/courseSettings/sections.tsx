import type { CourseSummary } from '@/lib/types';
import { Button } from '../ui/controls';
import { Dialog } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import type { CourseAction, CourseConfirmation } from './actions';
import { styles } from './styles';

interface CourseActionsProps {
  course: CourseSummary;
  busy: boolean;
  onAction: (action: CourseAction) => void;
}

export function CourseSyncActions({ course, busy, onAction }: CourseActionsProps) {
  return (
    <View style={styles.card}>
      <View style={styles.copy}>
        <Text style={styles.title}>Sync &amp; indexing</Text>
        <Text style={styles.description}>Refresh or rebuild this course's local data</Text>
      </View>
      <View style={styles.actions}>
        <Button id={`course-${course.id}-check`} disabled={busy} onClick={() => onAction('check')}>
          Check now
        </Button>
        <Button id={`course-${course.id}-reset`} disabled={busy} onClick={() => onAction('reset')}>
          Reset index
        </Button>
      </View>
    </View>
  );
}

export function CourseAccessActions({ course, busy, onAction }: CourseActionsProps) {
  return (
    <View style={{ ...styles.card, ...styles.danger }}>
      <View style={styles.copy}>
        <Text style={styles.title}>Course access</Text>
        <Text style={styles.description}>Hide it from the workspace or remove it completely</Text>
      </View>
      <View style={styles.actions}>
        <Button id={`course-${course.id}-hide`} disabled={busy} onClick={() => onAction('hide')}>
          Hide course
        </Button>
        <Button id={`course-${course.id}-delete`} tone="danger" disabled={busy} onClick={() => onAction('delete')}>
          Delete course
        </Button>
      </View>
    </View>
  );
}

export function CourseSettingsConfirm({
  courseId,
  confirmation,
  onCancel,
  onConfirm,
}: {
  courseId: string | number;
  confirmation: CourseConfirmation | null;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <Dialog
      id={`course-${courseId}-confirm-${confirmation?.action || 'closed'}`}
      title={confirmation?.title || ''}
      open={Boolean(confirmation)}
      onClose={onCancel}
      actions={
        <View style={styles.actions}>
          <Button id={`course-${courseId}-confirm-cancel`} onClick={onCancel}>
            Cancel
          </Button>
          <Button
            id={`course-${courseId}-confirm-${confirmation?.action || 'closed'}-submit`}
            tone={confirmation?.action === 'delete' ? 'danger' : 'primary'}
            onClick={onConfirm}
          >
            {confirmation?.label || ''}
          </Button>
        </View>
      }
    >
      <Text style={styles.description}>{confirmation?.message || ''}</Text>
    </Dialog>
  );
}
