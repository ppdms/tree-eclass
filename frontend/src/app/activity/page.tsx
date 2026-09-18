import { useLoaderData, useLocation } from 'react-router';
import ActivityPage from '@/features/activity/ActivityPage';
import type { loadActivity } from '@/app/data';

export default function ActivityRoute() {
  const data = useLoaderData<typeof loadActivity>();
  const { pathname, search } = useLocation();
  const value = new URLSearchParams(search).get('filter');
  const fallback =
    pathname === '/announcements'
      ? 'announcements'
      : pathname === '/timeline' || pathname === '/history'
        ? 'changes'
        : 'all';
  const filter =
    value === 'unread' || value === 'important' || value === 'announcements' || value === 'changes' ? value : fallback;
  const titles = new Map([
    ['/', 'Course updates'],
    ['/activity', 'Activity'],
    ['/announcements', 'Announcements'],
    ['/timeline', 'Timeline'],
    ['/history', 'History'],
  ]);
  return (
    <ActivityPage
      key={pathname + search}
      pageTitle={titles.get(pathname) || 'Activity'}
      initialFilter={filter}
      initialData={data}
    />
  );
}
