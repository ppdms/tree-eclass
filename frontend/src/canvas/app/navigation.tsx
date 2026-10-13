import { useEffect, useRef, useState } from 'react';
import { useLocation, useRouteLoaderData } from 'react-router';
import type { NavigationCourses } from '@/lib/types';
import { iconPath } from '@/components/iconPaths';
import { AppLink } from './links';
import { CourseNavigation } from './courseNavigation';
import { linkStyle, pages, section, selected } from './navigationStyles';
import { focusNode } from './browser';
import { subscribeCourseOrder } from './events';
import { useViewport } from './viewport';
import { Button } from '../ui/controls';
import { ScrollArea } from '../ui/overlays';
import { Text, Vector, View } from '../ui/primitives';
import { colors } from '../ui/palette';

const brand = iconPath('tree').match(/d="([^"]+)"/)?.[1];
if (!brand) throw new Error('Tree brand path is missing');

interface MobileLinksProps {
  pathname: string;
  close: () => void;
}
interface HeaderProps {
  pathname: string;
  courses: NavigationCourses['courses'];
  desktop: boolean;
  menu: boolean;
  toggleMenu: () => void;
}

function useCourses() {
  const data = useRouteLoaderData<NavigationCourses>('canvas-shell');
  const [courses, setCourses] = useState(data?.courses ?? []);
  useEffect(() => {
    setCourses(data?.courses ?? []);
  }, [data]);
  useEffect(() => subscribeCourseOrder(setCourses), []);
  return courses;
}
function SkipLink() {
  const [focused, setFocused] = useState(false);
  return (
    <AppLink
      id="skip-main"
      href="#app-root"
      fixed
      onFocusWithinChange={setFocused}
      onClick={() => focusNode('app-root')}
      style={{
        ...linkStyle,
        top: focused ? 8 : -100,
        zIndex: 1,
        left: 8,
        backgroundColor: colors.raised,
        borderWidth: 1,
        borderColor: colors.border,
      }}
    >
      Skip to main content
    </AppLink>
  );
}
function MobilePageLinks({ pathname, close }: MobileLinksProps) {
  return pages.map(([href, label]) => (
    <AppLink
      key={href}
      id={`mobile-nav-${label.toLowerCase()}`}
      href={href}
      style={selected(pathname, label)}
      onClick={close}
    >
      {label}
    </AppLink>
  ));
}
function MobileLinks({ pathname, close }: MobileLinksProps) {
  const viewport = useViewport();
  const first = useRef(true);
  const current = section(pathname);
  const initial = pages.some(([, label]) => label === current) ? current : 'Activity';
  const top = viewport.top + 82;
  return (
    <View
      id="nav-primary-menu"
      fixed
      blockPointer
      morphFrom="nav-menu"
      onLayout={() => {
        if (first.current) {
          first.current = false;
          focusNode(`mobile-nav-${initial.toLowerCase()}`);
        }
      }}
      onOutsidePointer={({ targetId }) => {
        if (targetId !== 'nav-menu') close();
      }}
      onKeyInput={(event) => {
        if (event.key === 'Escape' && !event.composing) {
          close();
          focusNode('nav-menu');
          return true;
        }
      }}
      style={{
        left: 16,
        right: 16,
        top,
        padding: 12,
        gap: 6,
        glass: 'overlay',
        backgroundColor: colors.surface,
        borderRadius: 20,
        borderWidth: 1,
        borderColor: colors.border,
      }}
    >
      <ScrollArea id="nav-primary-list" height="auto" maxHeight={Math.max(44, viewport.bottom - top - 24)}>
        <MobilePageLinks pathname={pathname} close={close} />
      </ScrollArea>
    </View>
  );
}
function BrandLink({ width }: { width: number }) {
  return (
    <AppLink
      id="nav-brand"
      href="/"
      block
      label="tree-eClass home"
      style={{ flexDirection: 'row', alignItems: 'center', gap: 8, padding: 6, minWidth: 0 }}
    >
      <Vector
        path={brand}
        fill={colors.success}
        viewBox={{ width: 300, height: 300 }}
        style={{ width: 24, height: 24 }}
      />
      {width >= 520 && <Text style={{ fontWeight: 650 }}>tree-eClass</Text>}
    </AppLink>
  );
}
function DesktopLinks({ pathname, courses }: Pick<HeaderProps, 'pathname' | 'courses'>) {
  return (
    <View style={{ flexDirection: 'row', alignItems: 'center', gap: 4, marginLeft: 'auto' }}>
      {pages.map(([href, label]) =>
        label === 'Courses' ? (
          <CourseNavigation key={href} pathname={pathname} courses={courses} />
        ) : (
          <AppLink key={href} id={`nav-${label.toLowerCase()}`} href={href} style={selected(pathname, label)}>
            {label}
          </AppLink>
        ),
      )}
    </View>
  );
}
function MobileMenuButton({ menu, toggleMenu }: Pick<HeaderProps, 'menu' | 'toggleMenu'>) {
  return (
    <Button
      id="nav-menu"
      expanded={menu}
      controls="nav-primary-menu"
      label={menu ? 'Close navigation menu' : 'Open navigation menu'}
      onClick={toggleMenu}
      style={{ ...linkStyle, color: colors.text, borderWidth: 0, backgroundColor: undefined }}
    >
      {menu ? 'Close' : 'Menu'}
    </Button>
  );
}
function NavigationHeader({ pathname, courses, desktop, menu, toggleMenu }: HeaderProps) {
  const viewport = useViewport();
  const current = ['announcements', 'timeline', 'history'].includes(pathname.slice(1))
    ? pathname.slice(1, 2).toUpperCase() + pathname.slice(2)
    : section(pathname);
  return (
    <View
      id="app-header"
      fixed
      style={{
        left: 16,
        top: viewport.top + 12,
        right: 16,
        height: 60,
        flexDirection: 'row',
        alignItems: 'center',
        gap: desktop ? 14 : 8,
        padding: 8,
        borderRadius: 28,
        borderWidth: 1,
        borderColor: colors.border,
        glass: 'bar',
        scrollEdge: true,
        backgroundColor: colors.surface,
      }}
    >
      <BrandLink width={viewport.width} />
      {!desktop && <Text style={{ minWidth: 0, flexShrink: 1, flexGrow: 1, fontWeight: 600 }}>{current}</Text>}
      {desktop && <DesktopLinks pathname={pathname} courses={courses} />}
      <AppLink
        id="nav-ask"
        href="/ask"
        style={{ ...selected(pathname, 'Ask'), marginLeft: 'auto' }}
        label="Ask your courses"
        title="Ask your courses (⌘K / Ctrl+K)"
      >
        Ask
      </AppLink>
      {!desktop && <MobileMenuButton menu={menu} toggleMenu={toggleMenu} />}
    </View>
  );
}
export function Navigation() {
  const { pathname } = useLocation();
  const viewport = useViewport();
  const desktop = viewport.width >= 1101;
  const [menu, setMenu] = useState(false);
  const courses = useCourses();
  useEffect(() => {
    setMenu(false);
  }, [pathname, desktop]);
  if (pathname.startsWith('/study/session')) return null;
  return (
    <>
      <SkipLink />
      <NavigationHeader
        pathname={pathname}
        courses={courses}
        desktop={desktop}
        menu={menu}
        toggleMenu={() => setMenu(!menu)}
      />
      {menu && !desktop && <MobileLinks pathname={pathname} close={() => setMenu(false)} />}
    </>
  );
}
