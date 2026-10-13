import type { CourseDetailPayload, RoadmapUnit } from '@/lib/types';
import { AppLink } from '../app/links';
import { Button, Progress } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { actionUnitTitle, courseStudyHref, overviewPercent, shortInstruction } from './model';
import { CourseReadiness } from './readiness';
import { styles } from './styles';

function ContinueCard({ data }: { data: CourseDetailPayload }) {
  const action = data.course_blueprint?.progress?.next_action;
  const title = actionUnitTitle(data.course_blueprint?.blueprint?.units || [], action);
  const minutes = action?.remaining_minutes || action?.estimated_minutes || 15;
  return (
    <View style={styles.continue}>
      <Text style={styles.eyebrow}>▶ Up next</Text>
      <Text style={styles.heroTitle}>{shortInstruction(action?.instruction)}</Text>
      <View style={styles.row}>
        {title && <Text style={styles.small}>{title}</Text>}
        <Text style={styles.small}>{minutes} min</Text>
      </View>
      <AppLink
        id={`course-${data.course.id}-start-session`}
        href={courseStudyHref(data.course.id)}
        style={{ ...styles.action, ...styles.primaryAction }}
      >
        Start session →
      </AppLink>
    </View>
  );
}

function ProgressCard({ data }: { data: CourseDetailPayload }) {
  const progress = data.course_blueprint?.progress || {};
  const percent = overviewPercent(data);
  return (
    <View style={styles.progress}>
      <Text style={styles.eyebrow}>Course progress</Text>
      <Text style={styles.progressValue}>{percent}%</Text>
      <Progress
        id={`course-${data.course.id}-overview-progress`}
        value={Math.min(100, percent)}
        label={`${percent}% complete`}
      />
      <Text style={styles.small}>
        {progress.completed_actions || 0} of {progress.total_actions || 0} actions complete
      </Text>
    </View>
  );
}

function UnitPreview({ unit, ordinal }: { unit: RoadmapUnit; ordinal: number }) {
  const completed = Number(unit.completed_actions || 0);
  const total = Number(unit.total_actions || 0);
  const percent = total ? Math.round((completed / total) * 100) : 0;
  return (
    <View style={styles.preview}>
      <Text style={styles.ordinal}>{ordinal}</Text>
      <View style={styles.copy}>
        <Text style={styles.unitTitle}>{unit.title}</Text>
        <Progress value={percent} label={`${unit.title || 'Topic'}: ${percent}% complete`} />
      </View>
      <Text style={styles.small}>
        {completed}/{total}
      </Text>
    </View>
  );
}

function RoadmapPreview({ data, onOpen }: { data: CourseDetailPayload; onOpen: () => void }) {
  const units = data.course_blueprint?.blueprint?.units || [];
  return (
    <View style={styles.section}>
      <View style={styles.sectionHeader}>
        <View style={styles.copy}>
          <Text style={styles.category}>Roadmap</Text>
          <Text style={styles.previewTitle}>{data.course_blueprint?.total_units ?? units.length} topics</Text>
        </View>
        <Button id={`course-${data.course.id}-view-roadmap`} onClick={onOpen}>
          View all →
        </Button>
      </View>
      {units.length ? (
        <View style={styles.previewList}>
          {units.slice(0, 6).map((unit, index) => (
            <UnitPreview key={unit.key || unit.title} unit={unit} ordinal={index + 1} />
          ))}
        </View>
      ) : (
        <CourseReadiness blueprint={data.course_blueprint} />
      )}
    </View>
  );
}

export function CourseOverview({ data, onOpenRoadmap }: { data: CourseDetailPayload; onOpenRoadmap: () => void }) {
  return (
    <View style={styles.dashboard}>
      <View style={styles.hero}>
        <ContinueCard data={data} />
        <ProgressCard data={data} />
      </View>
      <RoadmapPreview data={data} onOpen={onOpenRoadmap} />
    </View>
  );
}
