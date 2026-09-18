import { Link } from 'react-router';
import { useLocation } from 'react-router';
import { useEffect } from 'react';
import * as stylex from '@stylexjs/stylex';
import type { StyleXStyles } from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import { initNavChrome } from '@/shell/navChrome';
import { styles as shellStyles } from '@/styles/shell';
import type { NavigationCourses } from '@/lib/types';

function classNames(style: StyleXStyles, stableName: string): string {
  return [stylex.props(style).className, stableName].filter(Boolean).join(' ');
}

function routeState(pathname: string) {
  return {
    activity: ['/', '/activity', '/announcements', '/timeline', '/history'].includes(pathname),
    courses: pathname === '/courses' || pathname.startsWith('/courses/'),
    study: pathname === '/study' || pathname.startsWith('/study/session'),
    exercises: pathname === '/exercises',
    settings: pathname === '/settings',
    ask: pathname.startsWith('/ask'),
  };
}

function navLinkClass(isActive: boolean): string {
  return classNames(shellStyles.navLink, `nav-link${isActive ? ` ${classNames(shellStyles.navLinkCurrent, '')}` : ''}`);
}

function NavigationItem({ href, label, isActive }: { href: string; label: string; isActive: boolean }) {
  return (
    <li>
      <Link
        className={navLinkClass(isActive)}
        to={href}

        aria-current={isActive ? 'page' : undefined}
      >
        {label}
      </Link>
    </li>
  );
}

function CourseNavigationItem({ isActive, courses }: { isActive: boolean; courses: NavigationCourses['courses'] }) {
  return (
    <li className={classNames(shellStyles.orbit, 'nav-courses')}>
      <Link
        className={`${navLinkClass(isActive)} nav-course-trigger`}
        to="/courses"

        aria-haspopup="true"
        aria-expanded="false"
        aria-controls="nav-course-orbit"
        aria-current={isActive ? 'page' : undefined}
      >
        Courses
      </Link>
      <CourseOrbit courses={courses} />
    </li>
  );
}

function PrimaryLinks({ pathname, courses }: { pathname: string; courses: NavigationCourses['courses'] }) {
  const active = routeState(pathname);
  return (
    <ul
      className={classNames(shellStyles.navLinks, 'nav-links nav-primary')}
      id="nav-primary-menu"
      data-nav-open="false"
    >
      <NavigationItem href="/" label="Activity" isActive={active.activity} />
      <CourseNavigationItem isActive={active.courses} courses={courses} />
      <NavigationItem href="/study" label="Study" isActive={active.study} />
      <NavigationItem href="/exercises" label="Exercises" isActive={active.exercises} />
      <NavigationItem href="/settings" label="Settings" isActive={active.settings} />
    </ul>
  );
}

function CourseOrbit({ courses }: { courses: NavigationCourses['courses'] }) {
  return (
    <div
      className={classNames(shellStyles.orbitPanel, 'nav-course-orbit')}
      id="nav-course-orbit"
      data-course-nav={JSON.stringify(courses)}
      hidden
    >
      <p className={classNames(shellStyles.orbitStatus, 'nav-course-orbit-status')} role="status">
        Loading courses…
      </p>
      <ol className={classNames(shellStyles.orbitList, 'nav-course-orbit-list')} aria-label="Jump to a course" hidden />
    </div>
  );
}

function NavigationBrand({ pathname }: { pathname: string }) {
  const active = routeState(pathname);
  const currentView = active.activity
    ? pathname === '/timeline'
      ? 'Timeline'
      : pathname === '/history'
        ? 'History'
        : pathname === '/announcements'
          ? 'Announcements'
          : 'Activity'
    : active.courses
      ? 'Courses'
      : active.study
        ? 'Study'
        : active.exercises
          ? 'Exercises'
          : active.settings
            ? 'Settings'
            : active.ask
              ? 'Ask'
              : 'Activity';
  return (
    <div className={classNames(shellStyles.navBrand, 'nav-brand')}>
      <Link
        className={classNames(shellStyles.logoLink, 'logo-link')}
        to="/"

        aria-label="tree-eClass home"
      >
        <span className={classNames(shellStyles.logo, 'logo')} role="img">
          <Icon name="tree-fill" aria-hidden="true" />
          <span className={classNames(shellStyles.logoName, 'logo-name')}>tree-eClass</span>
        </span>
      </Link>
      <span className={classNames(shellStyles.navCurrentView, 'nav-current-view')} aria-hidden="true">
        {currentView}
      </span>
    </div>
  );
}

function NavigationUtility({ pathname }: { pathname: string }) {
  const active = routeState(pathname);
  return (
    <div className={classNames(shellStyles.navUtility, 'nav-utility')}>
      <Link
        className={`${classNames(shellStyles.navSearch, 'nav-search')}${
          active.ask ? ` ${classNames(shellStyles.navLinkCurrent, '')}` : ''
        }`}
        to="/ask"

        aria-label="Ask your courses"
        aria-current={active.ask ? 'page' : undefined}
      >
        <Icon name="search" aria-hidden="true" />
        <span className={classNames(shellStyles.navSearchLabel, '')}>Ask</span>
      </Link>
      <button
        type="button"
        className={classNames(shellStyles.navMenuButton, 'nav-menu-btn')}
        aria-expanded="false"
        aria-controls="nav-primary-menu"
        aria-label="Open navigation menu"
      >
        <Icon name="list" aria-hidden="true" />
      </button>
    </div>
  );
}

export function Navigation({ courses = [] }: NavigationCourses) {
  const pathname = useLocation().pathname;
  useEffect(() => initNavChrome(), []);
  useEffect(() => {
    document.dispatchEvent(new CustomEvent('treeEclass:courses-reordered', { detail: { courses } }));
  }, [courses]);
  const isSession = pathname.startsWith('/study/session');
  const skipLink = (
    <a className={classNames(shellStyles.skipLink, 'skip-link')} href="#app-root">
      Skip to main content
    </a>
  );
  return (
    <>
      {skipLink}
      <nav
        className={`${classNames(shellStyles.navbar, 'navbar')} ${
          isSession ? classNames(shellStyles.sessionNavbar, 'session-navbar') : ''
        }`}
        aria-label="Primary navigation"
      >
        <div className={classNames(shellStyles.container, 'container')}>
          <NavigationBrand pathname={pathname} />
          <PrimaryLinks pathname={pathname} courses={courses} />
          <NavigationUtility pathname={pathname} />
        </div>
      </nav>
    </>
  );
}
