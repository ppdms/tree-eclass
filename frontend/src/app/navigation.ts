import { matchRoutes, type RouteObject } from 'react-router';
import { router, routes } from './router';

export function navigate(href: string) {
  void router.navigate(href);
}

export function installNavigation() {
  document.addEventListener('click', (event) => {
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey
    )
      return;
    const target = event.target instanceof Element ? event.target.closest('a') : null;
    if (!target || target.hasAttribute('download') || target.target || target.rel.includes('external')) return;
    const url = new URL(target.href, location.href);
    if (url.origin !== location.origin) return;
    if (url.pathname === location.pathname && url.search === location.search && url.hash) return;
    const matches = matchRoutes<RouteObject>(routes, url.pathname);
    if (!matches || matches.at(-1)?.route.path === '*') return;
    event.preventDefault();
    navigate(url.pathname + url.search + url.hash);
  });
}
