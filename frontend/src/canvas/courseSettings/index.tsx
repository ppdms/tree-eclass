import type { CourseSummary } from '@/lib/types';
import { Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { CourseProfile } from './profile';
import { CourseAccessActions, CourseSettingsConfirm, CourseSyncActions } from './sections';
import { styles } from './styles';
import { useCourseActions } from './useCourseActions';

export function CourseSettings({ course }: { course: CourseSummary }) {
  const actions = useCourseActions(course);
  return (
    <View id={`course-${course.id}-settings`} style={styles.panel}>
      <View style={styles.copy}>
        <Text style={styles.category}>Preferences</Text>
        <Text style={styles.heading}>Course settings</Text>
      </View>
      <View style={styles.grid}>
        <CourseProfile course={course} busy={actions.busy} onRename={actions.rename} />
        <CourseSyncActions course={course} busy={actions.busy} onAction={actions.askAction} />
        <CourseAccessActions course={course} busy={actions.busy} onAction={actions.askAction} />
      </View>
      {actions.feedback && (
        <Notice id={`course-${course.id}-settings-error`} tone="danger">
          <Text>{actions.feedback}</Text>
        </Notice>
      )}
      <CourseSettingsConfirm
        courseId={course.id}
        confirmation={actions.confirmation}
        onCancel={actions.cancelConfirmation}
        onConfirm={actions.runAction}
      />
    </View>
  );
}
