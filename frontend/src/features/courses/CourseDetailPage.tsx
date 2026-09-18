import * as stylex from '@stylexjs/stylex';
import { courseDetailStyles } from './courseDetailStyles';
import * as React from 'react';
import { ArrowLeft, BookOpen, Files, History, Map, Play, Settings2, type LucideIcon } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Center } from '@/components/ui/layout';
import { buttonStyles } from '@/components/ui/styles';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { fetchJson } from '@/lib/api';
import { errorLikeSchema, errorMessage } from '@/lib/errors';
import type { CourseDetailPayload } from '@/lib/types';
import CourseFiles from './course-files';
import { LazyRoadmap, LazyUpdates } from './courseDeferredPanels';
import CourseSettings from './CourseSettings';
import { CourseOverview } from './courseDetailSections';

const styles = courseDetailStyles;

type CourseTab = 'overview' | 'roadmap' | 'settings' | 'files' | 'activity';

const COURSE_TABS: { id: CourseTab; label: string; icon: LucideIcon }[] = [
  { id: 'overview', label: 'Overview', icon: BookOpen },
  { id: 'roadmap', label: 'Roadmap', icon: Map },
  { id: 'files', label: 'Files', icon: Files },
  { id: 'activity', label: 'Updates', icon: History },
  { id: 'settings', label: 'Settings', icon: Settings2 },
];

function tabFromHash(): CourseTab {
  const hash = window.location.hash.slice(1);
  return isCourseTab(hash) ? hash : 'overview';
}

function isCourseTab(value: string): value is CourseTab {
  return COURSE_TABS.some((tab) => tab.id === value);
}

function useCourseTab(): [CourseTab, (tab: CourseTab) => void] {
  const [active, setActive] = React.useState<CourseTab>('overview');
  React.useEffect(() => {
    const sync = () => setActive(tabFromHash());
    sync();
    window.addEventListener('hashchange', sync);
    return () => window.removeEventListener('hashchange', sync);
  }, []);
  const select = React.useCallback((tab: CourseTab) => {
    setActive(tab);
    window.history.replaceState(window.history.state, '', tab === 'overview' ? '#overview' : `#${tab}`);
    window.scrollTo({ top: 0, behavior: 'auto' });
  }, []);
  return [active, select];
}

function CourseHeader({ data }: { data: CourseDetailPayload }) {
  const progress = data.course_blueprint?.progress || {};
  const hasNext = Boolean(progress.next_action);
  const completed = Number(progress.completed_actions || 0);
  const total = Number(progress.total_actions || 0);
  const percent = total ? Math.round((completed / total) * 100) : 0;
  return (
    <header {...stylex.props(styles.courseWorkspaceHeader)}>
      <div {...stylex.props(styles.courseWorkspaceIdentity)}>
        <Button
          variant="outline"
          size="icon"
          href="/courses"
          aria-label="Back to all courses"
          icon={<ArrowLeft size={16} />}
          {...stylex.props(styles.courseBackButton)}
        />
        <span {...stylex.props(styles.courseHeaderMark)}>{data.course.short_name || <BookOpen />}</span>
        <div {...stylex.props(styles.courseHeaderCopy)}>
          <h1 {...stylex.props(styles.courseHeaderTitle)}>{data.course.name}</h1>
          <div {...stylex.props(styles.courseHeaderProgress)} title={`${percent}% complete`}>
            <span {...stylex.props(styles.courseHeaderProgressTrack)}>
              <i {...stylex.props(styles.courseHeaderProgressFill(`${percent}%`))} />
            </span>
            <small>
              {completed}/{total}
            </small>
          </div>
        </div>
      </div>
      <Button
        variant="default"
        href={`/study?course_id=${data.course.id}`}
        icon={<Play size={14} fill="currentColor" />}
      >
        {hasNext ? 'Continue' : 'Study'}
      </Button>
    </header>
  );
}

function CourseTabList() {
  return (
    <TabsList {...stylex.props(styles.courseWorkspaceTabs)} aria-label="Course workspace">
      {COURSE_TABS.map(({ id, label, icon: Icon }) => (
        <TabsTrigger value={id} key={id} {...stylex.props(styles.courseWorkspaceTab)}>
          <Icon /> <span>{label}</span>
        </TabsTrigger>
      ))}
    </TabsList>
  );
}

function CoursePanels({
  data,
  active,
  openTab,
}: {
  data: CourseDetailPayload;
  active: CourseTab;
  openTab: (tab: CourseTab) => void;
}) {
  return (
    <>
      <TabsContent value="overview" {...stylex.props(styles.courseWorkspacePanel)}>
        <CourseOverview data={data} onOpenRoadmap={() => openTab('roadmap')} />
      </TabsContent>
      <TabsContent value="roadmap" {...stylex.props(styles.courseWorkspacePanel)}>
        {active === 'roadmap' ? <LazyRoadmap courseId={data.course.id} /> : null}
      </TabsContent>
      <TabsContent value="files" {...stylex.props(styles.courseWorkspacePanel)}>
        {active === 'files' ? <CourseFiles data={data} /> : null}
      </TabsContent>
      <TabsContent value="activity" {...stylex.props(styles.courseWorkspacePanel)}>
        {active === 'activity' ? <LazyUpdates courseId={data.course.id} /> : null}
      </TabsContent>
      <TabsContent value="settings" {...stylex.props(styles.courseWorkspacePanel)}>
        <CourseSettings course={data.course} />
      </TabsContent>
    </>
  );
}

function CourseSurface({ data }: { data: CourseDetailPayload }) {
  const [active, setActive] = useCourseTab();
  const select = (value: string) => {
    if (isCourseTab(value)) setActive(value);
  };
  return (
    <div {...stylex.props(styles.courseDetailPage)}>
      <CourseHeader data={data} />
      <Tabs value={active} onValueChange={select}>
        <CourseTabList />
        <CoursePanels data={data} active={active} openTab={setActive} />
      </Tabs>
    </div>
  );
}

export interface CourseDetailPageProps {
  courseId: string;
  initialData?: CourseDetailPayload | null;
}

function CourseLoadState({ error }: { error?: string | null }) {
  if (!error)
    return (
      <Center style={styles.courseLoading} role="status">
        Opening course…
      </Center>
    );
  const missing = /404|not found/i.test(error);
  return (
    <div {...stylex.props(styles.courseEmpty)} role="alert">
      <h1>Course unavailable</h1>
      <p>{missing ? 'This course does not exist or is no longer visible.' : 'The course overview could not load.'}</p>
      <div {...stylex.props(styles.appErrorActions)}>
        {!missing && (
          <button
            type="button"
            {...stylex.props(buttonStyles.base, buttonStyles.primary)}
            onClick={() => window.location.reload()}
          >
            Try again
          </button>
        )}
        <Button variant="outline" href="/courses">
          Return to courses
        </Button>
      </div>
    </div>
  );
}

export default function CourseDetailPage({ courseId, initialData = null }: CourseDetailPageProps) {
  const [data, setData] = React.useState<CourseDetailPayload | null>(initialData);
  const [error, setError] = React.useState<string | null>(null);
  React.useEffect(() => {
    const update = (event: Event) => {
      // SAFETY: the roadmap event form dispatches this named event with the typed overview response.
      const overview = (event as CustomEvent<CourseDetailPayload>).detail;
      if (String(overview?.course.id) === courseId) setData(overview);
    };
    window.addEventListener('tree:course-progress', update);
    return () => window.removeEventListener('tree:course-progress', update);
  }, [courseId]);
  React.useEffect(() => {
    if (initialData) return;
    setData(null);
    setError(null);
    fetchJson<CourseDetailPayload>(`api/v1/courses/${encodeURIComponent(courseId)}/overview`)
      .then(setData)
      .catch((err) => {
        const parsed = errorLikeSchema.safeParse(err);
        const errorLike = parsed.success ? parsed.data : { message: String(err) };
        setError(errorMessage(errorLike, 'Network connection issue'));
      });
  }, [courseId, initialData]);
  return data ? <CourseSurface data={data} /> : <CourseLoadState error={error} />;
}
