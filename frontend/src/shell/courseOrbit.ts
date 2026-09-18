import * as stylex from '@stylexjs/stylex';
import { iconPath } from '@/components/iconPaths';
import type { JsonValue } from '@/lib/display';
import { z } from 'zod/v4';
import { styles as shellStyles } from '@/styles/shell';

interface CourseNavItem {
  id: string | number;
  name: string;
}

interface OrbitElements {
  root: HTMLElement;
  trigger: HTMLAnchorElement;
  panel: HTMLElement;
  list: HTMLOListElement;
  status: HTMLElement;
}

const DESKTOP_NAV = '(min-width: 68.8125rem)';
const courseNavSchema = z.array(z.object({ id: z.union([z.string(), z.number()]), name: z.string() }));

function courseItems(value: JsonValue): CourseNavItem[] {
  const parsed = courseNavSchema.safeParse(value);
  return parsed.success ? parsed.data : [];
}

function renderCourseLinks(elements: OrbitElements, courses: CourseNavItem[]) {
  elements.list.replaceChildren();
  const visible = !elements.panel.hidden && elements.panel.style.opacity === '1';
  courses.forEach((course, index) => {
    const item = document.createElement('li');
    const link = document.createElement('a');
    const number = document.createElement('span');
    const name = document.createElement('span');
    const arrow = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    link.href = `/courses/${encodeURIComponent(String(course.id))}`;
    number.className = stylex.props(shellStyles.orbitIndex).className || '';
    number.textContent = String(index + 1).padStart(2, '0');
    name.className = stylex.props(shellStyles.orbitName).className || '';
    name.textContent = course.name;
    arrow.setAttribute('viewBox', '0 0 24 24');
    arrow.setAttribute('fill', 'none');
    arrow.setAttribute('stroke', 'currentColor');
    arrow.setAttribute('stroke-width', '1.8');
    arrow.setAttribute('stroke-linecap', 'round');
    arrow.setAttribute('stroke-linejoin', 'round');
    arrow.setAttribute('width', '1em');
    arrow.setAttribute('height', '1em');
    arrow.innerHTML = iconPath('arrow-up-right');
    arrow.setAttribute('aria-hidden', 'true');
    link.className = stylex.props(shellStyles.orbitLink).className || '';
    if (visible) {
      link.style.opacity = '1';
      link.style.transform = 'translateY(0)';
    }
    link.append(number, name, arrow);
    item.append(link);
    elements.list.append(item);
  });
  elements.status.hidden = courses.length > 0;
  elements.status.textContent = courses.length ? '' : 'No active courses';
  elements.list.hidden = courses.length === 0;
}

function findElements(): OrbitElements | null {
  const root = document.querySelector<HTMLElement>('.nav-courses');
  const trigger = root?.querySelector<HTMLAnchorElement>('.nav-course-trigger');
  const panel = root?.querySelector<HTMLElement>('.nav-course-orbit');
  const list = root?.querySelector<HTMLOListElement>('.nav-course-orbit-list');
  const status = root?.querySelector<HTMLElement>('.nav-course-orbit-status');
  return root && trigger && panel && list && status ? { root, trigger, panel, list, status } : null;
}

function orderedEventCourses(event: Event) {
  // SAFETY: this listener only receives the internal CustomEvent dispatched by
  // useCourseOrder, whose detail contains JSON-safe course identifiers and names.
  const detail = (event as CustomEvent<{ courses?: JsonValue }>).detail;
  return courseItems(detail?.courses ?? []);
}

function embeddedCourses(elements: OrbitElements): CourseNavItem[] {
  const raw = elements.panel.dataset.courseNav;
  if (!raw) return [];
  try {
    // SAFETY: this is the JSON string emitted by Navigation's data-course-nav
    // attribute; courseItems validates the parsed value before using it.
    return courseItems(JSON.parse(raw) as JsonValue);
  } catch {
    return [];
  }
}

function showOrbitPanel(elements: OrbitElements) {
  elements.panel.hidden = false;
  elements.panel.style.opacity = '1';
  elements.panel.style.pointerEvents = 'auto';
  elements.panel.style.transform = 'translateX(-50%) translateY(0) scale(1)';
  elements.list.querySelectorAll<HTMLElement>('a').forEach((link) => {
    link.style.opacity = '1';
    link.style.transform = 'translateY(0)';
  });
}

function hideOrbitPanel(elements: OrbitElements) {
  elements.panel.style.opacity = '0';
  elements.panel.style.pointerEvents = 'none';
  elements.panel.style.transform = 'translateX(-50%) translateY(-0.35rem) scale(0.985)';
  elements.list.querySelectorAll<HTMLElement>('a').forEach((link) => {
    link.style.opacity = '0';
    link.style.transform = 'translateY(-0.3rem)';
  });
}

function createOrbitControls(elements: OrbitElements, desktop: MediaQueryList) {
  renderCourseLinks(elements, embeddedCourses(elements));
  const loadPromise = Promise.resolve();
  let closeTimer = 0;
  let suppressFocusOpen = false;
  const open = async () => {
    if (!desktop.matches || suppressFocusOpen) return;
    window.clearTimeout(closeTimer);
    showOrbitPanel(elements);
    elements.trigger.setAttribute('aria-expanded', 'true');
    requestAnimationFrame(() => elements.root.setAttribute('data-open', 'true'));
    await loadPromise;
  };
  const close = (restoreFocus = false) => {
    window.clearTimeout(closeTimer);
    if (restoreFocus) {
      suppressFocusOpen = true;
      elements.trigger.focus();
      queueMicrotask(() => (suppressFocusOpen = false));
    }
    elements.root.removeAttribute('data-open');
    elements.trigger.setAttribute('aria-expanded', 'false');
    hideOrbitPanel(elements);
    closeTimer = window.setTimeout(() => (elements.panel.hidden = true), 160);
  };
  const scheduleClose = () => {
    window.clearTimeout(closeTimer);
    closeTimer = window.setTimeout(() => close(), 140);
  };
  return { open, close, scheduleClose };
}

function wireOrbitInteractions(
  elements: OrbitElements,
  desktop: MediaQueryList,
  controls: ReturnType<typeof createOrbitControls>,
) {
  elements.root.addEventListener('pointerenter', () => void controls.open());
  elements.root.addEventListener('pointerleave', controls.scheduleClose);
  elements.root.addEventListener('focusin', () => void controls.open());
  elements.root.addEventListener('focusout', (event) => {
    const nextTarget = event.relatedTarget;
    if (!(nextTarget instanceof Node) || !elements.root.contains(nextTarget)) {
      controls.scheduleClose();
    }
  });
  elements.root.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      controls.close(true);
    } else if (event.key === 'ArrowDown' && event.target === elements.trigger) {
      event.preventDefault();
      void controls.open().then(() => elements.list.querySelector<HTMLAnchorElement>('a')?.focus());
    }
  });
  desktop.addEventListener('change', () => {
    if (!desktop.matches) controls.close();
  });
}

function wireCourseOrderUpdates(elements: OrbitElements) {
  document.addEventListener('treeEclass:courses-reordered', (event) => {
    const courses = orderedEventCourses(event);
    renderCourseLinks(elements, courses);
  });
}

export function initCourseOrbit() {
  const elements = findElements();
  if (!elements) return;
  const desktop = window.matchMedia(DESKTOP_NAV);
  const controls = createOrbitControls(elements, desktop);
  wireOrbitInteractions(elements, desktop, controls);
  wireCourseOrderUpdates(elements);
}
