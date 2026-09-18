import { storageKey } from '@/lib/browserStorage';
import type {
  ActivityDay,
  ActivityFilter,
  ActivityGroup,
  ActivityGroupType,
  ActivityItem,
  CourseTimelineItem,
} from '@/lib/types';
import { isServer, lookup } from '@/lib/display';
import { formatActivityDay } from '@/lib/format';

const ACTIVITY_LABELS = {
  change: 'Course files',
  announcement: 'Announcement',
  assignment: 'Assignment',
} satisfies Record<string, string>;
export function activityLabel(type: string | undefined): string {
  return lookup(ACTIVITY_LABELS, type || '', 'Update');
}
export function activityHref(group: ActivityGroup, item: ActivityItem | undefined): string {
  if (item?.link) return item.link;
  if (group.link && group.link !== '#' && group.link !== '/#') return group.link;
  if (group.type === 'change' && item?.course_id && item?.change_no) {
    return `/courses/${item.course_id}/changes/${encodeURIComponent(item.change_no)}`;
  }
  return '/activity';
}
export function isExternalHref(href: string): boolean {
  return /^https?:\/\//i.test(String(href || ''));
}
export function dayLabel(group: ActivityGroup): string {
  const value = group.items?.[0]?.timestamp || group.timestamp;
  if (!value) return 'Undated';
  return formatActivityDay(value) || 'Undated';
}

export function groupByDay(groups: ActivityGroup[]): ActivityDay[] {
  return groups.reduce<ActivityDay[]>((days, group) => {
    const key = dayLabel(group);
    const day = days.find((entry) => entry.label === key);
    if (day) day.groups.push(group);
    else days.push({ label: key, groups: [group] });
    return days;
  }, []);
}

function timelineGroupType(item: CourseTimelineItem): ActivityGroupType {
  if (item.type === 'change') return 'change';
  if (item.type === 'assignment') return 'assignment';
  return 'announcement';
}

function timelineImportance(item: CourseTimelineItem): string {
  if (item.type === 'change') return 'informational';
  return /exam|deadline|cancel|urgent|important|submission/i.test(item.title || '') ? 'important' : 'informational';
}

export function timelineToActivityGroups(items: CourseTimelineItem[], courseId: string | number): ActivityGroup[] {
  const groups: ActivityGroup[] = [];
  const grouped = new Map<string, ActivityGroup>();
  items.forEach((item, index) => {
    const type = timelineGroupType(item);
    const title = item.title || item.message || 'Course update';
    const normalized = title.replace(/\s+/g, ' ').trim().toLowerCase();
    const id =
      type === 'change' ? `change:${item.id ?? item.sort_key ?? index}` : `${type}:${normalized.replaceAll(' ', '-')}`;
    const link =
      type === 'change' && item.change_no
        ? `/courses/${courseId}/changes/${encodeURIComponent(item.change_no)}`
        : item.link || `/courses/${courseId}#activity`;
    const existing = grouped.get(id);
    const activityItem = { ...item, type, link, course_id: item.course_id || courseId };
    if (existing) {
      existing.items?.push(activityItem);
      return;
    }
    const group: ActivityGroup = {
      id,
      type,
      title:
        type === 'change'
          ? `Course files changed in ${item.course_short_name || item.course_name || 'this course'}`
          : title,
      importance: timelineImportance(item),
      link,
      timestamp: item.timestamp || item.sort_key,
      items: [activityItem],
    };
    grouped.set(id, group);
    groups.push(group);
  });
  return groups;
}

export function readActivityIds(): Set<string | number> {
  if (isServer()) return new Set<string | number>();
  try {
    return new Set<string | number>(JSON.parse(localStorage.getItem(storageKey('treeeclass:activity-read')) || '[]'));
  } catch {
    return new Set<string | number>();
  }
}

export function filterActivity(
  groups: ActivityGroup[],
  filter: ActivityFilter,
  readIds?: Set<string | number>,
): ActivityGroup[] {
  if (filter === 'unread') return groups.filter((group) => !readIds?.has(group.id));
  if (filter === 'important') return groups.filter((group) => group.importance === 'important');
  if (filter === 'announcements') return groups.filter((group) => group.type === 'announcement');
  if (filter === 'changes') return groups.filter((group) => group.type === 'change');
  return groups;
}
