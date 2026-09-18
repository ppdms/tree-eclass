import * as stylex from '@stylexjs/stylex';
import type { ActivityGroup, ActivityItem as ActivityItemType } from '@/lib/types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { activityLabel } from './activityFeed';
import { ActivityReadToggle } from './ActivityReadToggle';

const styles = stylex.create({
  activityAsideCount: {
    borderColor: colors.border,
    borderRadius: layout.radiusPill,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '0.05rem',
    paddingInline: '0.45rem',
    backgroundColor: colors.surfaceRaised,
    color: colors.textPrimarySoft,
    fontSize: typography.sizeXs,
    fontVariantNumeric: 'tabular-nums',
    fontWeight: typography.weightSemibold,
    lineHeight: 1.2,
    whiteSpace: 'nowrap',
  },
  activityCourseLabel: {
    overflow: 'hidden',
    color: colors.textSecondary,
    fontSize: '0.625rem',
    fontWeight: 650,
    letterSpacing: { default: '0.06em', [media.mobile]: 'normal' },
    lineHeight: 1,
    textOverflow: 'ellipsis',
    textTransform: {
      default: 'uppercase',
      [media.mobile]: 'none',
    },
    whiteSpace: 'nowrap',
    maxWidth: { default: '8.5rem', [media.mobile]: '100%' },
  },
  activityItemAside: {
    gap: {
      default: '0.125rem',
      [media.mobile]: '0.5rem',
    },
    alignItems: {
      default: 'flex-end',
      [media.mobile]: 'center',
    },
    display: 'flex',
    flexBasis: 'auto',
    flexDirection: {
      default: 'column',
      [media.mobile]: 'row',
    },
    flexGrow: 0,
    flexShrink: 0,
    fontSize: '0.6875rem',
    gridColumnStart: {
      default: '3',
      [media.mobile]: '2',
    },
    gridRowStart: {
      default: '1',
      [media.mobile]: '2',
    },
    justifyContent: {
      default: 'flex-start',
      [media.mobile]: 'flex-end',
    },
    textAlign: {
      default: 'right',
      [media.mobile]: 'left',
    },
    minWidth: 0,
    width: {
      default: 'auto',
      [media.mobile]: '100%',
    },
  },
  asideControls: {
    flex: 'none',
    gap: '0.25rem',
    alignItems: 'center',
    display: 'flex',
  },
  activityItemMain: {
    gridColumnStart: '2',
    gridRowStart: '1',
    minWidth: 0,
  },
  activityState: {
    color: colors.textSecondary,
    display: 'block',
    fontSize: '0.625rem',
    fontWeight: 650,
    letterSpacing: '0.08em',
    lineHeight: 1.2,
    textTransform: 'uppercase',
    marginBottom: '0.125rem',
  },
  activityTitle: {
    textDecoration: 'none',
    color: {
      default: colors.textPrimary,
      ':hover': colors.success,
    },
    display: 'block',
    fontSize: typography.sizeBase,
    fontWeight: typography.weightSemibold,
    letterSpacing: '-0.02em',
    lineHeight: 1.25,
    overflowWrap: 'break-word',
    transitionDuration: '150ms',
    transitionProperty: 'color',
  },
});

function ActivityAsideControls({ group, fileCount }: { group: ActivityGroup; fileCount: number }) {
  return (
    <div {...stylex.props(styles.asideControls)}>
      {fileCount > 0 && (
        <span {...stylex.props(styles.activityAsideCount)}>
          {fileCount} {fileCount === 1 ? 'file' : 'files'}
        </span>
      )}
      <ActivityReadToggle groupId={group.id} title={group.title} />
    </div>
  );
}

function ActivityCourseLabel({ courseLabel }: { courseLabel: string }) {
  if (!courseLabel) return null;
  return (
    <span {...stylex.props(styles.activityCourseLabel)} title={courseLabel}>
      {courseLabel}
    </span>
  );
}

type ActivityAsideProps = {
  group: ActivityGroup;
  item: ActivityItemType | undefined;
  courseLabel: string;
};

export function ActivityAside(props: ActivityAsideProps) {
  const fileCount = props.group.type === 'change' ? props.item?.changes?.length || 0 : 0;
  return <ActivityAsideLayout {...props} fileCount={fileCount} />;
}

function ActivityAsideLayout({ group, courseLabel, fileCount }: ActivityAsideProps & { fileCount: number }) {
  return (
    <aside {...stylex.props(styles.activityItemAside)} aria-label="Update details">
      <ActivityAsideControls group={group} fileCount={fileCount} />
      <ActivityCourseLabel courseLabel={courseLabel} />
    </aside>
  );
}

// Keep the public adapter small; the layout owns the responsive presentation.
export function ActivityItemMain({ group, href, external }: { group: ActivityGroup; href: string; external: boolean }) {
  return (
    <div {...stylex.props(styles.activityItemMain)}>
      <span {...stylex.props(styles.activityState)}>
        {activityLabel(group.type)}
        {group.importance === 'important' ? ' · Important' : ''}
      </span>
      <a
        {...stylex.props(styles.activityTitle)}
        href={href}
        target={external ? '_blank' : undefined}
        rel={external ? 'noopener noreferrer' : undefined}
      >
        {group.title}
      </a>
    </div>
  );
}
