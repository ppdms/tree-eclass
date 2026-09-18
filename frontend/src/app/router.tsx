import { createBrowserRouter, redirect } from 'react-router';
import { Layout } from './layout';
import { RouteError } from './routeError';
import { Loading } from './Loading';
import * as data from './data';

const activity = {
  loader: data.loadActivity,
  lazy: async () => ({ Component: (await import('./activity/page')).default }),
};

export const routes = [
  {
    Component: Layout,
    loader: data.loadNavigationCourses,
    shouldRevalidate: () => true,
    ErrorBoundary: RouteError,
    HydrateFallback: Loading,
    children: [
      ...['/', '/activity', '/announcements', '/timeline', '/history'].map((path) => ({ path, ...activity })),
      {
        path: '/courses',
        loader: data.loadCourses,
        lazy: async () => ({ Component: (await import('./courses/page')).default }),
      },
      {
        path: '/courses/:courseId',
        loader: data.courseLoader,
        lazy: async () => ({ Component: (await import('./courses/[courseId]/page')).default }),
      },
      {
        path: '/courses/:courseId/changes/:changeNo',
        loader: data.changeLoader,
        lazy: async () => ({ Component: (await import('./courses/[courseId]/changes/[changeNo]/page')).default }),
      },
      {
        path: '/study',
        loader: data.studyLoader,
        lazy: async () => ({ Component: (await import('./study/page')).default }),
      },
      {
        path: '/study/session',
        loader: ({ request }: { request: Request }) =>
          new URL(request.url).searchParams.has('course_id') ? null : redirect('/study'),
        lazy: async () => ({ Component: (await import('./study/session/page')).default }),
      },
      {
        path: '/exercises',
        loader: data.loadExercises,
        lazy: async () => ({ Component: (await import('./exercises/page')).default }),
      },
      {
        path: '/settings',
        loader: data.loadSettings,
        lazy: async () => ({ Component: (await import('./settings/page')).default }),
      },
      { path: '/ask', loader: data.askLoader, lazy: async () => ({ Component: (await import('./ask/page')).default }) },
      { path: '*', lazy: async () => ({ Component: (await import('./not-found')).default }) },
    ].map((route) => ({ ...route, ErrorBoundary: RouteError })),
  },
];

export const router = createBrowserRouter(routes);
