import type { ActivityGroup } from '@/lib/types';
import { activityHref, activityLabel, isExternalHref } from '@/features/activity/activityFeed';
import { Icon, Text, View } from '../ui/primitives';
import { ActivityStore, useReadIds } from './data';
import { ChangeDiffTree } from '../changes/diff';
import { ActivityLink } from './links';
import { colors, styles } from './styles';

interface ItemProps {
  group: ActivityGroup;
  store: ActivityStore;
  scope: string;
}

function ReadToggle({ group, store, scope }: ItemProps) {
  const read = useReadIds(store).has(group.id);
  return (
    <View
      id={`activity-read-${scope}-${group.id}`}
      onClick={() => store.toggle(group.id)}
      style={{ ...styles.toggle, color: read ? colors.success : colors.muted }}
    >
      <Icon name={read ? 'mail-open' : 'check'} style={styles.toggleIcon} />
      <Text style={styles.toggleText}>{read ? 'Read' : 'Mark read'}</Text>
    </View>
  );
}

function ActivityAside({ group, store, scope }: ItemProps) {
  const item = group.items?.[0];
  const count = group.type === 'change' ? item?.changes?.length || 0 : 0;
  const course = item?.course_short_name || item?.course_name || (group.type === 'change' ? 'Course files' : '');
  return (
    <View style={styles.aside}>
      <View style={styles.controls}>
        {count > 0 && (
          <Text style={styles.fileCount}>
            {count} {count === 1 ? 'file' : 'files'}
          </Text>
        )}
        <ReadToggle group={group} store={store} scope={scope} />
      </View>
      {course && <Text style={styles.course}>{course}</Text>}
    </View>
  );
}

function ActivityDescription({ description }: { description: string }) {
  const text = description
    .replace(/<\/?(?:p|div|li|br)\b[^>]*>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\u00a0/g, ' ')
    .replace(/[ \t]+/g, ' ')
    .replace(/[ \t]*\n[ \t]*/g, '\n')
    .replace(/\n{2,}/g, '\n\n')
    .trim();
  return <Text style={styles.description}>{text}</Text>;
}

export function ActivityItem({ group, store, scope }: ItemProps) {
  const item = group.items?.[0];
  const href = activityHref(group, item);
  const external = isExternalHref(href);
  const read = useReadIds(store).has(group.id);
  const important = group.importance === 'important';
  return (
    <View
      id={`activity-${scope}-${group.id}`}
      style={{ ...styles.item, ...(important ? styles.importantItem : undefined), opacity: read ? 0.68 : 1 }}
    >
      <View style={{ ...styles.marker, ...(important ? styles.importantMarker : undefined) }}>
        <Icon name={group.type === 'change' ? 'folder-open' : 'megaphone'} style={styles.markerIcon} />
      </View>
      <View style={styles.itemMain}>
        <Text style={styles.state}>
          {activityLabel(group.type)}
          {important ? ' · Important' : ''}
        </Text>
        <ActivityLink
          id={`activity-title-${scope}-${group.id}`}
          href={href}
          external={external}
          target={external ? '_blank' : undefined}
          rel={external ? 'noopener noreferrer' : undefined}
          style={styles.itemTitle}
        >
          {group.title}
        </ActivityLink>
      </View>
      <ActivityAside group={group} store={store} scope={scope} />
      {item?.description && (
        <View style={styles.itemBody}>
          <ActivityDescription description={item.description} />
        </View>
      )}
      {(item?.type === 'change' || group.type === 'change') && Boolean(item?.changes?.length) && (
        <View style={styles.itemBody}>
          <ChangeDiffTree changes={item?.changes || []} scope={encodeURIComponent(JSON.stringify([scope, group.id]))} />
        </View>
      )}
    </View>
  );
}
