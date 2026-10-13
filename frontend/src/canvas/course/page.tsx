import { lazy, Suspense, useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router';
import type { CourseDetailPayload } from '@/lib/types';
import { scrollPageTo } from '../app/browser';
import { subscribeCourseProgress } from '../app/events';
import { CourseSettings } from '../courseSettings';
import { Text, View } from '../ui/primitives';
import { CourseHeader, CourseTabs } from './header';
import { tabFromHash, type CourseTab } from './model';
import { CourseOverview } from './overview';
import { styles } from './styles';

const CourseFiles = lazy(() => import('../courseFiles').then((module) => ({ default: module.CourseFiles })));
const CourseRoadmap = lazy(() => import('../roadmap').then((module) => ({ default: module.CourseRoadmap })));
const CourseUpdates = lazy(() => import('../changes/updates').then((module) => ({ default: module.CourseUpdates })));

function CoursePanel({
  data,
  active,
  onSelect,
}: {
  data: CourseDetailPayload;
  active: CourseTab;
  onSelect: (tab: CourseTab) => void;
}) {
  if (active === 'overview') return <CourseOverview data={data} onOpenRoadmap={() => onSelect('roadmap')} />;
  if (active === 'settings') return <CourseSettings course={data.course} />;
  if (active === 'files') return <CourseFiles data={data} />;
  if (active === 'roadmap') return <CourseRoadmap courseId={data.course.id} />;
  return <CourseUpdates courseId={data.course.id} />;
}

export function CoursePage({ data }: { data: CourseDetailPayload }) {
  const [progress, setProgress] = useState<{
    source: CourseDetailPayload;
    overview: CourseDetailPayload;
  } | null>(null);
  const overview = progress?.source === data ? progress.overview : data;
  const location = useLocation();
  const navigate = useNavigate();
  const active = tabFromHash(location.hash);
  useEffect(
    () =>
      subscribeCourseProgress((next) => {
        if (String(next.course.id) === String(data.course.id)) setProgress({ source: data, overview: next });
      }),
    [data],
  );
  const select = (tab: CourseTab) => {
    const hash = `#${tab}`;
    if (location.hash !== hash) {
      void navigate(
        { pathname: location.pathname, search: location.search, hash },
        {
          replace: true,
          state: location.state,
          preventScrollReset: true,
        },
      );
    }
    scrollPageTo(0);
  };
  const loading =
    active === 'roadmap' ? 'Loading roadmap…' : active === 'files' ? 'Loading course files…' : 'Loading updates…';
  return (
    <View id={`course-${data.course.id}-page`} ready style={styles.page}>
      <CourseHeader data={overview} />
      <CourseTabs courseId={data.course.id} active={active} onSelect={select} />
      <View key={active} id={`course-${data.course.id}-panel-${active}`}>
        <Suspense fallback={<Text style={styles.description}>{loading}</Text>}>
          <CoursePanel data={overview} active={active} onSelect={select} />
        </Suspense>
      </View>
    </View>
  );
}
