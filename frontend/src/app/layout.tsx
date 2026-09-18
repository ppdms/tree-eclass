import { useEffect } from 'react';
import { Outlet, ScrollRestoration, useLoaderData, useLocation, useNavigation } from 'react-router';
import { Navigation } from '@/components/shell/Navigation';
import { ShellMain } from '@/components/shell/ShellMain';
import { RuntimeNotice } from '@/components/shell/RuntimeNotice';
import type { loadNavigationCourses } from './data';

export function Layout() {
  const { courses } = useLoaderData<typeof loadNavigationCourses>();
  const location = useLocation();
  const navigation = useNavigation();
  useEffect(() => {
    const section = location.pathname.split('/')[1] || 'activity';
    document.title = `${section.charAt(0).toUpperCase() + section.slice(1)} · tree-eClass`;
  }, [location.pathname]);
  return (
    <>
      <Navigation courses={courses} />
      <RuntimeNotice />
      <ShellMain>
        {navigation.state !== 'idle' && (
          <p role="status" aria-live="polite">
            Opening the page…
          </p>
        )}
        <Outlet />
      </ShellMain>
      <ScrollRestoration />
    </>
  );
}
