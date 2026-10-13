import { useMemo } from 'react';
import type { ActivityFilter, ActivityGroup } from '@/lib/types';
import { groupByDay } from '@/features/activity/activityFeed';
import { Text, View } from '../ui/primitives';
import { ActivityFeed, ActivityStore, type FeedSnapshot } from './data';
import { ActivityItem } from './items';
import { ActivityLink } from './links';
import { styles } from './styles';

interface ListProps {
  groups: ActivityGroup[];
  filter: ActivityFilter;
  store: ActivityStore;
  scope: string;
}

function ActivityEmpty({ filter }: { filter: ActivityFilter }) {
  const copy =
    filter === 'important'
      ? 'Nothing important yet. Deadline and course-change updates appear here automatically.'
      : filter === 'unread'
        ? 'You have read every update.'
        : 'Nothing in this view yet.';
  return (
    <View style={styles.empty}>
      <Text style={styles.emptyCopy}>{copy}</Text>
      {filter !== 'all' && (
        <ActivityLink href="/" style={styles.action}>
          Show all updates
        </ActivityLink>
      )}
      {filter === 'all' && (
        <ActivityLink href="/courses" style={styles.action}>
          Open Courses
        </ActivityLink>
      )}
    </View>
  );
}

export function ActivityList({ groups, filter, store, scope }: ListProps) {
  const days = useMemo(() => groupByDay(groups), [groups]);
  if (!groups.length) return <ActivityEmpty filter={filter} />;
  return (
    <View style={styles.list}>
      {days.map((day) => (
        <View key={day.label}>
          <Text style={styles.dayHeading}>{day.label}</Text>
          <View>
            {day.groups.map((group) => (
              <ActivityItem key={group.id} group={group} store={store} scope={scope} />
            ))}
          </View>
        </View>
      ))}
    </View>
  );
}

function ActivityStream(props: ListProps & { title: string; description: string }) {
  return (
    <View style={styles.stream}>
      <View style={styles.streamHeading}>
        <Text style={styles.streamTitle}>{props.title}</Text>
        <Text style={styles.streamDescription}>{props.description}</Text>
      </View>
      <ActivityList groups={props.groups} filter={props.filter} store={props.store} scope={props.scope} />
    </View>
  );
}

export function ActivityStreams({ groups, filter, store }: Omit<ListProps, 'scope'>) {
  const [changes, updates] = useMemo(
    () =>
      [groups.filter((group) => group.type === 'change'), groups.filter((group) => group.type !== 'change')] as const,
    [groups],
  );
  return (
    <View id="activity-content">
      <View style={styles.streams}>
        <ActivityStream
          groups={changes}
          filter={filter}
          store={store}
          scope="files"
          title="Course files"
          description="Changes from your courses"
        />
        <ActivityStream
          groups={updates}
          filter={filter}
          store={store}
          scope="updates"
          title="Announcements & other updates"
          description="Announcements, assignments, and everything else"
        />
      </View>
      <View style={styles.singleStream}>
        <ActivityList groups={groups} filter={filter} store={store} scope="mobile" />
      </View>
    </View>
  );
}

export function FeedStatus({ feed, state }: { feed: ActivityFeed; state: FeedSnapshot }) {
  if (state.error) {
    return (
      <View id="activity-feed-error" style={styles.error}>
        <Text style={styles.status}>
          {state.error === 'older' ? 'Older updates could not load.' : 'Course updates could not load.'}
        </Text>
        <View
          id="activity-retry"
          onClick={() => {
            void feed.retry();
          }}
          style={styles.action}
        >
          <Text>Try again</Text>
        </View>
      </View>
    );
  }
  if (!state.loaded) return <Text style={styles.status}>Fetching course updates…</Text>;
  if (!state.hasMore) return <Text style={styles.status}>All updates loaded.</Text>;
  return (
    <View
      id="activity-load-sentinel"
      onVisible={
        state.loading
          ? undefined
          : () => {
              void feed.loadOlder();
            }
      }
      onClick={() => {
        void feed.loadOlder();
      }}
      style={styles.sentinel}
    >
      <Text style={styles.status}>{state.loading ? 'Fetching older updates…' : 'Scroll for older updates'}</Text>
    </View>
  );
}
