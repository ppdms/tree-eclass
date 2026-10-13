import { useMemo } from 'react';
import type { ChangeDetailPayload, ChangeItem } from '@/lib/types';
import { AppLink } from '../app/links';
import { Text, View } from '../ui/primitives';
import { ChangeDiffTree, CHANGE_PRESENTATION } from './diff';
import { changeCounts, changeKind, changeLabel, changeScope } from './model';
import { styles } from './styles';

export interface ChangeDetailProps {
  courseId: string;
  changeNo: string;
  data: ChangeDetailPayload;
}

function ChangeSummary({ changes }: { changes: ChangeItem[] }) {
  const counts = useMemo(() => changeCounts(changes), [changes]);
  return (
    <View label="Change summary" style={styles.summary}>
      {counts.map(([type, count]) => {
        const presentation = CHANGE_PRESENTATION[changeKind(type)];
        return (
          <View key={type} style={styles.summaryStat}>
            <Text style={{ ...styles.summarySign, color: presentation.color }}>{presentation.symbol}</Text>
            <Text style={styles.summaryLabel}>
              {count} {changeLabel(type)}
            </Text>
          </View>
        );
      })}
    </View>
  );
}

function ChangeHeader({ courseId, changeNo, data }: ChangeDetailProps) {
  const record = data.change_record || {};
  const courseHref = `/courses/${encodeURIComponent(courseId)}`;
  const scope = changeScope(courseId, changeNo);
  return (
    <View style={styles.header}>
      <View label="Breadcrumb" style={styles.breadcrumb}>
        <AppLink id={`change-activity-${scope}`} href="/activity" style={styles.breadcrumbLink}>
          Activity
        </AppLink>
        <Text style={styles.breadcrumbText}>/</Text>
        <AppLink id={`change-course-${scope}`} href={courseHref} style={styles.breadcrumbLink}>
          {data.course?.name || 'Course'}
        </AppLink>
        <Text style={styles.breadcrumbText}>/ Change {changeNo}</Text>
      </View>
      <Text style={styles.title}>Course files changed</Text>
      <Text style={styles.message}>{record.message || 'The course source tree was updated.'}</Text>
      <View style={styles.metadata}>
        <Text style={styles.muted}>{record.timestamp || 'Timestamp unavailable'}</Text>
        <Text style={styles.muted}>{data.webdav_folder || 'Course storage'}</Text>
      </View>
      <ChangeSummary changes={data.changes || []} />
    </View>
  );
}

export function ChangeDetail(props: ChangeDetailProps) {
  const scope = changeScope(props.courseId, props.changeNo);
  return (
    <View id={`change-detail-${scope}`} ready style={styles.page}>
      <ChangeHeader {...props} />
      <View style={styles.card}>
        <Text id={`change-files-title-${scope}`} style={styles.sectionTitle}>
          Changed files
        </Text>
        <ChangeDiffTree changes={props.data.changes || []} webdavFolder={props.data.webdav_folder} scope={scope} />
      </View>
      <AppLink
        id={`change-back-${scope}`}
        href={`/courses/${encodeURIComponent(props.courseId)}`}
        style={styles.action}
      >
        Back to course
      </AppLink>
    </View>
  );
}
