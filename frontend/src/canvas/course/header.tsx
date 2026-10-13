import type { CourseDetailPayload } from '@/lib/types';
import { AppLink } from '../app/links';
import { Button, Progress } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { COURSE_TABS, courseStudyHref, headerProgress, type CourseTab } from './model';
import { styles } from './styles';

export function CourseHeader({ data }: { data: CourseDetailPayload }) {
  const { completed, total, percent } = headerProgress(data);
  return (
    <View id={`course-${data.course.id}-header`} style={styles.header}>
      <View style={styles.identity}>
        <AppLink id={`course-${data.course.id}-back`} href="/courses" label="Back to all courses" style={styles.action}>
          ←
        </AppLink>
        <Text style={styles.mark}>{data.course.short_name || 'Course'}</Text>
        <View style={styles.copy}>
          <Text style={styles.title}>{data.course.name}</Text>
          <View style={styles.headerProgress}>
            <View style={styles.headerTrack}>
              <Progress
                id={`course-${data.course.id}-header-progress`}
                value={percent}
                label={`${percent}% complete`}
              />
            </View>
            <Text style={styles.small}>
              {completed}/{total}
            </Text>
          </View>
        </View>
      </View>
      <AppLink
        id={`course-${data.course.id}-study`}
        href={courseStudyHref(data.course.id)}
        style={{ ...styles.action, ...styles.primaryAction }}
      >
        {data.course_blueprint?.progress?.next_action ? 'Continue' : 'Study'}
      </AppLink>
    </View>
  );
}

interface CourseTabsProps {
  courseId: string | number;
  active: CourseTab;
  onSelect: (tab: CourseTab) => void;
}

export function CourseTabs({ courseId, active, onSelect }: CourseTabsProps) {
  return (
    <View id={`course-${courseId}-tabs`} style={styles.tabs}>
      {COURSE_TABS.map((tab) => (
        <Button
          key={tab.id}
          id={`course-${courseId}-tab-${tab.id}`}
          pressed={active === tab.id}
          controls={`course-${courseId}-panel-${tab.id}`}
          style={active === tab.id ? styles.tabActive : undefined}
          onClick={() => onSelect(tab.id)}
        >
          {tab.label}
        </Button>
      ))}
    </View>
  );
}
