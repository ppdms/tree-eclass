import * as stylex from '@stylexjs/stylex';
import { activityPartsStyles } from './activityPartsStyles';
import * as React from 'react';
import { Icon } from '@/components/Icon';
import { buttonStyles } from '@/components/ui/styles';
import { DiffTree } from './DiffTree';
import { groupByDay, activityHref, isExternalHref } from './activityFeed';
import { ActivityAside, ActivityItemMain } from './activityItemParts';
import type {
  ActivityDay as ActivityDayType,
  ActivityFilter,
  ActivityGroup,
  ActivityItem as ActivityItemType,
} from '@/lib/types';

const styles = activityPartsStyles;

function ActivityDiffTree({ item }: { item: ActivityItemType | undefined }) {
  if (item?.type !== 'change' || !item?.changes?.length) return null;
  return (
    <div {...stylex.props(styles.activityDiffTree)}>
      <DiffTree changes={item.changes} />
    </div>
  );
}

function ActivityDescription({ description }: { description: string }) {
  return (
    <p {...stylex.props(styles.activityDescription)}>
      {description
        .replace(/<\/?(?:p|div|li|br)\b[^>]*>/gi, '\n')
        .replace(/<[^>]+>/g, ' ')
        .replace(/\u00a0/g, ' ')
        .replace(/[ \t]+/g, ' ')
        .replace(/[ \t]*\n[ \t]*/g, '\n')
        .replace(/\n{2,}/g, '\n\n')
        .trim()}
    </p>
  );
}

export interface ActivityItemProps {
  group: ActivityGroup;
}

function ActivityItem({ group }: ActivityItemProps) {
  const item = group.items?.[0];
  const icon = group.type === 'change' ? 'folder2-open' : 'megaphone';
  const courseLabel = item?.course_short_name || item?.course_name || (group.type === 'change' ? 'Course files' : '');
  const href = activityHref(group, item);
  const external = isExternalHref(href);
  return (
    <article
      {...stylex.props(styles.activityItem, group.importance === 'important' && styles.activityItemImportant)}
      data-activity-id={group.id}
      data-activity-importance={group.importance}
    >
      <ActivityItemMarker icon={icon} important={group.importance === 'important'} />
      <ActivityItemMain group={group} href={href} external={external} />
      <ActivityAside courseLabel={courseLabel} group={group} item={item} />
      {group.type === 'announcement' && item?.description && <ActivityDescription description={item.description} />}
      <ActivityDiffTree item={item} />
    </article>
  );
}

function ActivityItemMarker({ icon, important }: { icon: string; important: boolean }) {
  return (
    <div {...stylex.props(styles.activityItemMarker, important && styles.markerImportant)}>
      <Icon name={icon} aria-hidden="true" />
    </div>
  );
}

export function ActivityDay({
  day,
  headingLevel = 2,
  isFirst = false,
}: {
  day: ActivityDayType;
  headingLevel?: 2 | 3;
  isFirst?: boolean;
}) {
  const DayHeading = headingLevel === 3 ? 'h3' : 'h2';
  return (
    <section {...stylex.props(styles.activityDay, isFirst && styles.activityDayFirst)}>
      <DayHeading {...stylex.props(styles.dayHeading)}>{day.label}</DayHeading>
      <div>
        {day.groups.map((group) => (
          <ActivityItem key={group.id} group={group} />
        ))}
      </div>
    </section>
  );
}

export function ActivityEmpty({ filter }: { filter?: ActivityFilter }) {
  const copy =
    filter === 'important'
      ? 'Nothing important yet. Deadline and course-change updates appear here automatically.'
      : filter === 'unread'
        ? 'You have read every update.'
        : 'Nothing in this view yet.';
  return (
    <div {...stylex.props(styles.activityEmpty)}>
      <p {...stylex.props(styles.emptyCopy)}>{copy}</p>
      <ActivityEmptyAction filter={filter} />
    </div>
  );
}

function ActivityEmptyAction({ filter }: { filter?: ActivityFilter }) {
  return filter && filter !== 'all' ? (
    <a href="/" {...stylex.props(buttonStyles.base, buttonStyles.secondary)}>
      Show all updates
    </a>
  ) : (
    <a {...stylex.props(buttonStyles.base, buttonStyles.secondary)} href="/courses">
      Open Courses
    </a>
  );
}

export interface ActivityListProps {
  groups: ActivityGroup[];
  filter?: ActivityFilter;
  headingLevel?: 2 | 3;
}

export function ActivityList({ groups, filter, headingLevel = 2 }: ActivityListProps) {
  if (!groups.length) return <ActivityEmpty filter={filter} />;
  return <ActivityDayList groups={groups} headingLevel={headingLevel} />;
}

export function ActivityDayList({ groups, headingLevel }: { groups: ActivityGroup[]; headingLevel?: 2 | 3 }) {
  return (
    <div {...stylex.props(styles.activityList)}>
      {groupByDay(groups).map((day, idx) => (
        <ActivityDay key={day.label} day={day} headingLevel={headingLevel} isFirst={idx === 0} />
      ))}
    </div>
  );
}
