import { matchRoutes, type RouteObject, type createMemoryRouter } from 'react-router';

type Router = ReturnType<typeof createMemoryRouter>;
let active: Router | undefined;
let routes: RouteObject[] = [];
let installed = false;

export function navigate(href: string) {
  void active?.navigate(href);
}

/** Preview-only browser adapter; application routes, loaders and components are unchanged. */
export function configureNavigation(router: Router, applicationRoutes: RouteObject[]) {
  active = router;
  routes = applicationRoutes;
  if (installed) return;
  installed = true;
  document.addEventListener('click', (event) => {
    if (event.defaultPrevented || event.button || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey)
      return;
    const link = event.target instanceof Element ? event.target.closest('a') : null;
    if (!link || link.hasAttribute('download') || link.target || link.rel.includes('external')) return;
    const url = new URL(link.href, location.href);
    if (url.origin !== location.origin || (url.hash && url.pathname === location.pathname)) return;
    const matches = matchRoutes(routes, url.pathname);
    if (!matches || matches.at(-1)?.route.path === '*') return;
    event.preventDefault();
    navigate(url.pathname + url.search + url.hash);
  });
}
