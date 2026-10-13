import type { ReactNode } from 'react';
import { createMemoryRouter, RouterProvider, type DataRouter, type RouteObject } from 'react-router';
import { ActivityFeed, ActivityStore, ActivityStoreContext } from '../activity/data';
import { Text } from '../ui/primitives';
import { AppShell, MissingPage, RouteError } from './shell';
import { requestJson } from './api';
import { subscribeCatalogChanged } from './events';
import { historyUserState } from './historyState';
import type { HistoryState } from './protocol';
import type { NavigationCourses } from '@/lib/types';
import {
  loadAsk,
  loadChange,
  loadCourseDetail,
  loadCourses,
  loadExercises,
  loadSessionQuery,
  loadSettings,
  loadStudy,
} from './loaders';

interface BrowserNavigation {
  url: string;
  action: 'PUSH' | 'REPLACE' | 'POP';
  key: string;
  state: HistoryState;
}
export interface RouteCommit extends BrowserNavigation {
  native: boolean;
  scroll: number | false | null;
  preventScrollReset: boolean;
  hashOnly: boolean;
}
export interface CanvasApplication {
  element: ReactNode;
  navigate: (url: string) => void;
  receiveNavigation: (message: BrowserNavigation) => void;
}
const aliases = [
  { id: 'home', index: true },
  { id: 'activity', path: 'activity' },
  { id: 'announcements', path: 'announcements' },
  { id: 'timeline', path: 'timeline' },
  { id: 'history', path: 'history' },
];

function logicalUrl(value: string, base = '/'): string {
  const url = new URL(value, new URL(base, 'https://canvas.invalid'));
  return url.pathname + url.search + url.hash;
}
function lazy(path: string, loader: RouteObject['loader'], load: () => Promise<RouteObject>): RouteObject {
  return {
    path,
    loader,
    lazy: async () => {
      const route = await load();
      return {
        Component: route.Component,
        ErrorBoundary: route.ErrorBoundary ?? RouteError,
        HydrateFallback: route.HydrateFallback,
        shouldRevalidate: route.shouldRevalidate,
      };
    },
  };
}
const featureRoutes = [
  lazy('courses', loadCourses, async () => (await import('../courses/route')).coursesRoute),
  lazy('courses/:courseId', loadCourseDetail, async () => (await import('../course/route')).courseDetailRoute),
  lazy('courses/:courseId/changes/:changeNo', loadChange, async () => (await import('../changes/route')).changeRoute),
  lazy('exercises', loadExercises, async () => (await import('../exercises/route')).exercisesRoute),
  lazy('study', loadStudy, async () => (await import('../study/route')).studyRoute),
  lazy('study/session', loadSessionQuery, async () => (await import('../session/route')).sessionRoute),
  lazy('settings', loadSettings, async () => (await import('../settings/route')).settingsRoute),
  lazy('ask', loadAsk, async () => (await import('../ask/route')).askRoute),
];
async function navigationCourses({ request }: { request: Request }): Promise<NavigationCourses> {
  try {
    return await requestJson<NavigationCourses>('/api/v1/navigation/courses', { signal: request.signal });
  } catch (error) {
    if (request.signal.aborted) throw error;
    return { courses: [] };
  }
}
function appRoutes(feed: ActivityFeed, store: ActivityStore): RouteObject[] {
  return [
    {
      id: 'canvas-shell',
      path: '/',
      loader: navigationCourses,
      shouldRevalidate: () => true,
      element: <AppShell />,
      errorElement: (
        <AppShell>
          <RouteError />
        </AppShell>
      ),
      hydrateFallbackElement: (
        <AppShell loaded={false}>
          <Text style={{ padding: 24 }}>Opening page…</Text>
        </AppShell>
      ),
      children: [
        {
          id: 'activity-layout',
          loader: ({ request }) => feed.loadFirst(request.signal),
          lazy: async () => {
            const { ActivityPage } = await import('../activity/page');
            const Component = () => <ActivityPage feed={feed} store={store} />;
            return { Component, HydrateFallback: Component };
          },
          children: aliases,
        },
        ...featureRoutes,
        { path: '*', element: <MissingPage /> },
      ],
    },
  ];
}
/** Route data starts independently of GPU/font initialization. Browser history owns external POP entries. */
export function createCanvasApp(
  initialUrl: string,
  store: ActivityStore,
  onNavigate: (route: RouteCommit) => void,
  onData: () => void,
  initialState: HistoryState,
  initialKey: string,
  scroll: { positions: Record<string, number>; read: () => number },
): CanvasApplication {
  const feed = new ActivityFeed(onData);
  const initial = new URL(logicalUrl(initialUrl), 'https://canvas.invalid');
  const router = createMemoryRouter(appRoutes(feed, store), {
    initialEntries: [
      {
        pathname: initial.pathname,
        search: initial.search,
        hash: initial.hash,
        state: historyUserState(initialState),
        key: initialKey,
      },
    ],
  });
  const receiveNavigation = connectHistory(router, initialKey, scroll, onNavigate);
  let dataReady = false;
  subscribeCatalogChanged(() => {
    void router.revalidate();
  });
  router.subscribe((state) => {
    if (!state.initialized) return;
    if (!dataReady) {
      dataReady = true;
      onData();
    }
  });
  return {
    element: (
      <ActivityStoreContext value={store}>
        <RouterProvider router={router} />
      </ActivityStoreContext>
    ),
    navigate: (url) => {
      void router.navigate(logicalUrl(url, locationUrl(router.state.location)));
    },
    receiveNavigation,
  };
}

function locationUrl(location: { pathname: string; search: string; hash: string }): string {
  return location.pathname + location.search + location.hash;
}
function connectHistory(
  router: DataRouter,
  initialKey: string,
  scroll: { positions: Record<string, number>; read: () => number },
  onNavigate: (route: RouteCommit) => void,
) {
  let committed = router.state.location;
  let mirrored: BrowserNavigation | null = null;
  const keys = new Map([[initialKey, initialKey]]);
  // Use the same restoration contract as React Router's DOM adapter, with native POP entry identities.
  router.enableScrollRestoration(
    scroll.positions,
    scroll.read,
    (location) => keys.get(location.key) ?? (mirrored?.url === locationUrl(location) ? mirrored.key : location.key),
  );
  router.subscribe((state) => {
    if (!state.initialized || state.location.key === committed.key) return;
    const url = locationUrl(state.location);
    const external = mirrored?.url === url ? mirrored : null;
    const hashOnly =
      committed.pathname === state.location.pathname &&
      committed.search === state.location.search &&
      committed.hash !== state.location.hash;
    committed = state.location;
    keys.set(committed.key, external?.key ?? committed.key);
    mirrored = null;
    onNavigate({
      url,
      native: external !== null,
      action: external?.action ?? state.historyAction,
      key: external?.key ?? committed.key,
      state: external?.state ?? { usr: committed.state },
      scroll: state.restoreScrollPosition,
      preventScrollReset: state.preventScrollReset,
      hashOnly,
    });
  });
  return (message: BrowserNavigation) => {
    mirrored = { ...message, url: logicalUrl(message.url) };
    void router.navigate(mirrored.url, { replace: true, state: historyUserState(message.state) });
  };
}
