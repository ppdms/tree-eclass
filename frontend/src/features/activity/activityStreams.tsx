import * as stylex from '@stylexjs/stylex';
import type { ActivityFilter, ActivityGroup } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import { ActivityList } from './activityParts';

const styles = stylex.create({
  activityStream: {
    minWidth: 0,
  },
  activityStreamHeader: {
    gap: '1rem',
    alignItems: 'flex-start',
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: '0.65rem',
  },
  streamTitle: {
    margin: 0,
    color: colors.textPrimary,
    fontSize: typography.sizeLg,
    fontWeight: 650,
    letterSpacing: '-0.02em',
    lineHeight: 1.2,
  },
  streamDesc: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    lineHeight: 1.35,
    marginTop: '0.2rem',
  },
  activityStreams: {
    gap: '1.5rem',
    alignItems: 'start',
    display: {
      default: 'none',
      [media.desktop]: 'grid',
    },
    gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
    minWidth: 0,
  },
  activityMobileList: {
    display: {
      default: 'block',
      [media.desktop]: 'none',
    },
    minWidth: 0,
  },
});

interface ActivityStreamProps {
  streamId: string;
  title: string;
  description: string;
  groups: ActivityGroup[];
  filter?: ActivityFilter;
}

function ActivityStream({ streamId, title, description, groups, filter }: ActivityStreamProps) {
  return (
    <section {...stylex.props(styles.activityStream)} aria-labelledby={streamId}>
      <header {...stylex.props(styles.activityStreamHeader)}>
        <div>
          <h2 id={streamId} {...stylex.props(styles.streamTitle)}>
            {title}
          </h2>
          <p {...stylex.props(styles.streamDesc)}>{description}</p>
        </div>
      </header>
      <ActivityList groups={groups} filter={filter} headingLevel={3} />
    </section>
  );
}

export interface ActivityStreamsProps {
  groups: ActivityGroup[];
  filter?: ActivityFilter;
}

function ActivityStreamGrid({ groups, filter }: ActivityStreamsProps) {
  const streams = [
    {
      streamId: 'activity-stream-changes',
      title: 'Course files',
      description: 'Changes from your courses',
      groups: groups.filter((group) => group.type === 'change'),
    },
    {
      streamId: 'activity-stream-updates',
      title: 'Announcements & other updates',
      description: 'Announcements, assignments, and everything else',
      groups: groups.filter((group) => group.type !== 'change'),
    },
  ];
  return (
    <div {...stylex.props(styles.activityStreams)}>
      {streams.map((stream) => (
        <ActivityStream key={stream.streamId} {...stream} filter={filter} />
      ))}
    </div>
  );
}

function ActivityMobileList({ groups, filter }: ActivityStreamsProps) {
  return (
    <div {...stylex.props(styles.activityMobileList)}>
      <ActivityList groups={groups} filter={filter} />
    </div>
  );
}

export function ActivityStreams({ groups, filter }: ActivityStreamsProps) {
  return (
    <>
      <ActivityStreamGrid groups={groups} filter={filter} />
      <ActivityMobileList groups={groups} filter={filter} />
    </>
  );
}
