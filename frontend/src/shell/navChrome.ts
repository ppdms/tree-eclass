import { navigate } from '@/app/navigation';
// Server-rendered nav chrome interactions (formerly frontend/dist/client/script-core.js).
// Runs on every shell page through the mobile nav menu and global Ask shortcut.
// The shell markup is server-rendered, so this wires existing DOM, not React
// components.

import { storageKey } from '@/lib/browserStorage';
import { isServer } from '@/lib/display';
import { iconPath } from '@/components/iconPaths';
import { themeClasses } from '@/styles/theme';
import { initCourseOrbit } from './courseOrbit';

const THEME_STORAGE_KEY = 'treeEclass.theme';
const THEME_VALID = new Set(['system', 'light', 'dark', 'eink']);

export type ThemeName = 'system' | 'light' | 'dark' | 'eink';

function preferredTheme(): ThemeName {
  if (isServer()) return 'system';
  try {
    const stored = localStorage.getItem(storageKey(THEME_STORAGE_KEY));
    // SAFETY: THEME_VALID.has above established that the stored value is
    // one of the four theme names; the cast narrows the string.
    return THEME_VALID.has(stored || '') ? (stored as ThemeName) : 'system';
  } catch {
    return 'system';
  }
}

function resolvedTheme(name: ThemeName): ThemeName {
  if (name !== 'system') return name;
  return window.matchMedia?.('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

function writeStoredTheme(name: ThemeName) {
  try {
    localStorage.setItem(storageKey(THEME_STORAGE_KEY), name);
  } catch {
    // Private mode / quota — the theme still applies for this session.
  }
}

export function getThemePreference(): ThemeName {
  return preferredTheme();
}

export function setTheme(name: ThemeName) {
  if (!THEME_VALID.has(name)) return;
  const resolved: ThemeName = resolvedTheme(name);
  document.documentElement.setAttribute('data-astryx-theme', 'neutral');
  document.documentElement.setAttribute('data-theme', resolved);
  document.documentElement.className = themeClasses[resolved === 'system' ? 'dark' : resolved] || '';
  writeStoredTheme(name);
  document.dispatchEvent(new CustomEvent('treeEclass:theme', { detail: { theme: name, resolved } }));
}

function initNavMenu() {
  const trigger = document.querySelector<HTMLButtonElement>('.nav-menu-btn');
  const menu = document.getElementById('nav-primary-menu');
  if (!trigger || !menu) return;
  const icon = trigger.querySelector<SVGSVGElement>('[data-nav-icon]');
  const setIcon = (name: 'list' | 'close') => {
    if (!icon) return;
    icon.dataset.navIcon = name;
    icon.innerHTML = iconPath(name);
  };
  const close = (restoreFocus = false) => {
    if (menu.dataset.navOpen !== 'true') return;
    menu.dataset.navOpen = 'false';
    menu.style.display = '';
    trigger.setAttribute('aria-expanded', 'false');
    trigger.setAttribute('aria-label', 'Open navigation menu');
    setIcon('list');
    if (restoreFocus) trigger.focus();
  };
  const open = () => {
    menu.dataset.navOpen = 'true';
    menu.style.display = 'grid';
    trigger.setAttribute('aria-expanded', 'true');
    trigger.setAttribute('aria-label', 'Close navigation menu');
    setIcon('close');
    menu.querySelector<HTMLElement>('a[aria-current="page"], a')?.focus();
  };
  trigger.addEventListener('click', () => (menu.dataset.navOpen === 'true' ? close() : open()));
  menu.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      close(true);
    }
  });
  menu.addEventListener('click', (event) => {
    // SAFETY: click events target DOM elements; the cast narrows the
    // generic EventTarget to read closest().
    if ((event.target as HTMLElement).closest('a')) close();
  });
  document.addEventListener('click', (event) => {
    // SAFETY: click events target DOM elements; the cast narrows the
    // generic EventTarget for contains().
    if (!menu.contains(event.target as Node) && !trigger.contains(event.target as Node)) close();
  });
}

function initAskShortcut() {
  document.addEventListener('keydown', (event) => {
    if (
      event.defaultPrevented ||
      event.isComposing ||
      event.key.toLowerCase() !== 'k' ||
      !(event.metaKey || event.ctrlKey) ||
      event.shiftKey ||
      event.altKey
    )
      return;
    if (
      ['INPUT', 'SELECT', 'TEXTAREA'].includes(document.activeElement?.tagName || '') ||
      // SAFETY: activeElement is an Element when present; the cast narrows
      // it to read isContentEditable.
      (document.activeElement as HTMLElement | null)?.isContentEditable
    )
      return;
    event.preventDefault();
    const input = document.getElementById('ask-question');
    if (input) input.focus();
    else navigate('/ask?focus=1');
  });
}

export function initNavChrome() {
  initNavMenu();
  initCourseOrbit();
  initAskShortcut();
}
