import { useEffect, useRef, useState } from 'react';
import type { NavigationCourses } from '@/lib/types';
import { AppLink } from './links';
import { focusNode } from './browser';
import { useViewport } from './viewport';
import { linkStyle, selected } from './navigationStyles';
import { ScrollArea } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { useNodeRect } from '../ui/layoutEvents';
import type { CanvasKey } from '../ui/types';

interface CourseNavigationProps {
  pathname: string;
  courses: NavigationCourses['courses'];
}
interface CourseOrbitProps {
  courses: NavigationCourses['courses'];
  close: () => void;
  onLayout: () => void;
}

function CourseLinks({ courses, close }: Pick<CourseOrbitProps, 'courses' | 'close'>) {
  return courses.length ? (
    courses.map((course, index) => (
      <AppLink
        key={String(course.id)}
        id={`nav-course-${course.id}`}
        href={`/courses/${encodeURIComponent(course.id)}`}
        style={linkStyle}
        onClick={close}
      >
        {String(index + 1).padStart(2, '0')} {course.name} ↗
      </AppLink>
    ))
  ) : (
    <Text style={{ ...linkStyle, color: colors.muted }}>No active courses</Text>
  );
}
function CourseOrbit({ courses, close, onLayout }: CourseOrbitProps) {
  const viewport = useViewport();
  const anchor = useNodeRect('nav-courses');
  const width = Math.min(440, viewport.width - 32);
  const left = Math.max(16, Math.min((anchor?.x ?? 16) - width / 3, viewport.width - width - 16));
  return (
    <View
      id="nav-course-orbit"
      fixed
      blockPointer
      morphFrom="nav-courses"
      onLayout={onLayout}
      style={{
        left,
        top: (anchor?.y ?? 24) + (anchor?.height ?? 44) + 8,
        width,
        padding: 12,
        gap: 6,
        borderRadius: 20,
        glass: 'overlay',
        backgroundColor: colors.surface,
        borderWidth: 1,
        borderColor: colors.border,
      }}
    >
      <ScrollArea id="nav-course-list" height="auto" maxHeight={Math.max(80, viewport.bottom - 130)}>
        <CourseLinks courses={courses} close={close} />
      </ScrollArea>
    </View>
  );
}
function useCourseMenu({ pathname, courses }: CourseNavigationProps) {
  const [open, setOpen] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const suppressed = useRef(false);
  const first = useRef(false);
  const close = () => {
    clearTimeout(timer.current);
    setOpen(false);
  };
  const within = (present: boolean) => {
    clearTimeout(timer.current);
    if (present && !suppressed.current) setOpen(true);
    else if (!present) timer.current = setTimeout(close, 140);
  };
  useEffect(() => {
    setOpen(false);
    return () => clearTimeout(timer.current);
  }, [pathname]);
  const focusFirst = () => {
    if (!first.current || !courses.length) return;
    first.current = false;
    focusNode(`nav-course-${courses[0]!.id}`);
  };
  const onKeyInput = (event: CanvasKey) => {
    if (event.composing) return;
    if (event.key === 'Escape' && open) {
      suppressed.current = true;
      close();
      focusNode('nav-courses');
      queueMicrotask(() => {
        suppressed.current = false;
      });
      return true;
    }
    if (event.key === 'ArrowDown') {
      first.current = true;
      setOpen(true);
      if (open) focusFirst();
      return true;
    }
  };
  return { open, close, within, focusFirst, onKeyInput };
}
export function CourseNavigation(props: CourseNavigationProps) {
  const menu = useCourseMenu(props);
  return (
    <View onHoverChange={menu.within} onFocusWithinChange={menu.within} onKeyInput={menu.onKeyInput}>
      <AppLink
        id="nav-courses"
        href="/courses"
        style={selected(props.pathname, 'Courses')}
        expanded={menu.open}
        controls="nav-course-orbit"
        onClick={menu.close}
      >
        Courses
      </AppLink>
      {menu.open && <CourseOrbit courses={props.courses} close={menu.close} onLayout={menu.focusFirst} />}
    </View>
  );
}
