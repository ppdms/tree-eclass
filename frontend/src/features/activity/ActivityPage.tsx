import * as stylex from '@stylexjs/stylex';
import type { ActivityFilter, InboxPayload } from '@/lib/types';
import { commonStyles } from '@/styles/common';
import { filterActivity } from './activityFeed';
import { ActivityFeedLoader } from './ActivityFeedLoader';
import { ActivityFilterTabs } from './ActivityFilterTabs';
import { ActivityStreams } from './activityStreams';

const styles = stylex.create({
  activityInbox: {
    marginInline: 'auto',
    marginBottom: '3rem',
    maxWidth: '73.75rem',
    minWidth: 0,
    width: '100%',
  },
});

interface ActivityPageProps {
  initialFilter?: ActivityFilter;
  initialData?: InboxPayload | null;
  pageTitle?: string;
}

export default function ActivityPage({
  initialFilter = 'all',
  initialData = null,
  pageTitle = 'Course updates',
}: ActivityPageProps) {
  const allGroups = initialData?.groups || [];
  const groups = filterActivity(allGroups, initialFilter);

  return (
    <div {...stylex.props(styles.activityInbox)}>
      <h1 id="activity-inbox-title" {...stylex.props(commonStyles.srOnly)}>
        {pageTitle}
      </h1>
      <ActivityFilterTabs currentFilter={initialFilter} />
      <ActivityStreams groups={groups} filter={initialFilter} />
      <ActivityFeedLoader initialOffset={initialData?.next_offset ?? 30} hasMore={Boolean(initialData?.has_more)} />
    </div>
  );
}
