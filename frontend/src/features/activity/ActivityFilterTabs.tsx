import * as stylex from '@stylexjs/stylex';
import { Link } from 'react-router';
import { useLocation, useSearchParams } from 'react-router';
import type { ActivityFilter } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  tabsNav: {
    marginBlockEnd: '1.25rem',
    overflowX: 'auto',
  },
  tabList: {
    margin: 0,
    padding: 0,
    gap: '0.375rem',
    listStyle: 'none',
    alignItems: 'center',
    display: 'flex',
    minWidth: 'min-content',
  },
  tabItem: {
    margin: 0,
    padding: 0,
    display: 'inline-flex',
  },
  tabLink: {
    borderColor: colors.border,
    borderRadius: layout.radiusPill,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '0.375rem',
    paddingInline: '0.875rem',
    textDecoration: 'none',
    alignItems: 'center',
    backgroundColor: {
      default: colors.surface,
      ':hover': colors.surfaceRaised,
    },
    color: colors.textSecondary,
    cursor: 'pointer',
    display: 'inline-flex',
    fontSize: typography.sizeSm,
    fontWeight: typography.weightMedium,
    lineHeight: 1.2,
    transitionDuration: '140ms',
    transitionProperty: 'background-color, border-color, color',
    whiteSpace: 'nowrap',
  },
  tabLinkActive: {
    borderColor: colors.textPrimary,
    backgroundColor: colors.surfaceRaised,
    color: colors.textPrimary,
    fontWeight: typography.weightSemibold,
  },
});

interface FilterItem {
  id: ActivityFilter;
  label: string;
}

const FILTER_ITEMS: FilterItem[] = [
  { id: 'all', label: 'All updates' },
  { id: 'unread', label: 'Unread' },
  { id: 'important', label: 'Important' },
  { id: 'announcements', label: 'Announcements' },
  { id: 'changes', label: 'Course files' },
];

function buildFilterHref(pathname: string, filterId: ActivityFilter): string {
  const targetPath = pathname.startsWith('/announcements') || pathname.startsWith('/timeline') ? '/' : pathname;
  if (filterId === 'all') return targetPath || '/';
  const query = new URLSearchParams();
  query.set('filter', filterId);
  return `${targetPath || '/'}?${query.toString()}`;
}

function parseFilterParam(value: string | null | undefined): ActivityFilter | null {
  if (
    value === 'all' ||
    value === 'unread' ||
    value === 'important' ||
    value === 'announcements' ||
    value === 'changes'
  ) {
    return value;
  }
  return null;
}

function FilterTab({ item, isActive, href }: { item: FilterItem; isActive: boolean; href: string }) {
  return (
    <li {...stylex.props(styles.tabItem)} role="presentation">
      <Link
        to={href}
        role="tab"
        aria-selected={isActive}

        {...stylex.props(styles.tabLink, isActive && styles.tabLinkActive)}
      >
        {item.label}
      </Link>
    </li>
  );
}

export function ActivityFilterTabs({ currentFilter = 'all' }: { currentFilter?: ActivityFilter }) {
  const pathname = useLocation().pathname;
  const [searchParams] = useSearchParams();
  const activeFilter = parseFilterParam(searchParams?.get('filter')) || currentFilter;

  return (
    <nav aria-label="Filter course updates" {...stylex.props(styles.tabsNav)}>
      <ul {...stylex.props(styles.tabList)} role="tablist">
        {FILTER_ITEMS.map((item) => {
          const isActive = item.id === activeFilter;
          const href = buildFilterHref(pathname, item.id);
          return <FilterTab key={item.id} item={item} isActive={isActive} href={href} />;
        })}
      </ul>
    </nav>
  );
}
