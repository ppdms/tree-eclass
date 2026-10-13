import { useEffect, useMemo } from 'react';
import { useLocation } from 'react-router';
import type { ActivityFilter } from '@/lib/types';
import { filterActivity } from '@/features/activity/activityFeed';
import { lookup } from '@/lib/display';
import { Text, View } from '../ui/primitives';
import { ActivityFeed, ActivityStore, useActivityFeed, useReadIds } from './data';
import { ActivityStreams, FeedStatus } from './feed';
import { ActivityLink } from './links';
import { styles } from './styles';

const TITLES = {
  '/': 'Course updates',
  '/activity': 'Activity',
  '/announcements': 'Announcements',
  '/timeline': 'Timeline',
  '/history': 'History',
} satisfies Record<string, string>;

const FILTERS: { id: ActivityFilter; label: string }[] = [
  { id: 'all', label: 'All updates' },
  { id: 'unread', label: 'Unread' },
  { id: 'important', label: 'Important' },
  { id: 'announcements', label: 'Announcements' },
  { id: 'changes', label: 'Course files' },
];

export function routeFilter(pathname: string, search: string): ActivityFilter {
  const value = new URLSearchParams(search).get('filter');
  if (value === 'unread' || value === 'important' || value === 'announcements' || value === 'changes') return value;
  if (pathname === '/announcements') return 'announcements';
  return pathname === '/timeline' || pathname === '/history' ? 'changes' : 'all';
}

function ActivityFilterTabs({ current, pathname }: { current: ActivityFilter; pathname: string }) {
  const contextRoute = pathname === '/announcements' || pathname === '/timeline' || pathname === '/history';
  const target = contextRoute ? '/' : pathname;
  return (
    <View id="activity-filters" style={styles.tabs}>
      {FILTERS.map((filter) => (
        <ActivityLink
          key={filter.id}
          id={`activity-filter-${filter.id}`}
          href={filter.id === 'all' ? target : `${target}?filter=${filter.id}`}
          style={{ ...styles.tab, ...(current === filter.id ? styles.tabActive : undefined) }}
        >
          {filter.label}
        </ActivityLink>
      ))}
    </View>
  );
}

export function ActivityPage({ feed, store }: { feed: ActivityFeed; store: ActivityStore }) {
  const { pathname, search } = useLocation();
  const filter = routeFilter(pathname, search);
  const state = useActivityFeed(feed);
  const readIds = useReadIds(store);
  const groups = useMemo(() => filterActivity(state.groups, filter, readIds), [state.groups, filter, readIds]);
  useEffect(
    () => () => {
      // The router owns first-page cancellation; unmount only cancels appended-page requests.
      if (feed.getSnapshot().loaded) feed.cancel();
    },
    [feed],
  );
  return (
    <View id="activity-root" ready={state.loaded || state.error !== null} style={styles.root}>
      <View style={styles.page}>
        <View style={styles.intro}>
          <Text id="activity-inbox-title" style={styles.title}>
            {lookup(TITLES, pathname, 'Activity')}
          </Text>
        </View>
        <ActivityFilterTabs current={filter} pathname={pathname} />
        {state.loaded && <ActivityStreams groups={groups} filter={filter} store={store} />}
        <FeedStatus feed={feed} state={state} />
      </View>
    </View>
  );
}
