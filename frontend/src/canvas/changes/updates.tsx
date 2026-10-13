import { useMemo } from 'react';
import { timelineToActivityGroups } from '@/features/activity/activityFeed';
import { useActivityStore } from '../activity/data';
import { ActivityList } from '../activity/feed';
import { Button } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { changeScope } from './model';
import { styles } from './styles';
import { useCourseUpdates } from './useUpdates';

export interface CourseUpdatesProps {
  courseId: number | string;
}

function CourseUpdatesBody({ courseId }: CourseUpdatesProps) {
  const store = useActivityStore();
  const state = useCourseUpdates(courseId);
  const groups = useMemo(() => timelineToActivityGroups(state.items, courseId), [state.items, courseId]);
  const scope = `course-updates-${changeScope(courseId)}`;
  return (
    <View id={scope} label="Course updates" ready={!state.busy || state.items.length > 0} style={styles.updates}>
      <ActivityList groups={groups} filter="all" store={store} scope={scope} />
      {state.busy && <Text style={styles.muted}>Loading updates…</Text>}
      {state.error && (
        <View style={{ gap: 8 }}>
          <Text style={styles.error}>Updates could not load.</Text>
          <Button id={`${scope}-retry`} onClick={state.retry} disabled={state.busy}>
            Try again
          </Button>
        </View>
      )}
      {!state.error && state.hasMore && (
        <Button id={`${scope}-more`} onClick={state.loadMore} disabled={state.busy}>
          Load more
        </Button>
      )}
    </View>
  );
}

export function CourseUpdates({ courseId }: CourseUpdatesProps) {
  return <CourseUpdatesBody key={changeScope(courseId)} courseId={courseId} />;
}
