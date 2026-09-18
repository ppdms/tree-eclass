import * as stylex from '@stylexjs/stylex';
import { getThemePreference, setTheme } from './navChrome';
import { RUNTIME_GUARD_SCRIPT } from './runtimeGuard';
import { styles as shellStyles } from '@/styles/shell';
import { startDevelopmentReload } from './developmentReload';

export function initializeBrowser() {
  clearDevelopmentStorage();
  setTheme(getThemePreference());
  document.body.className = stylex.props(shellStyles.body).className || '';
  const guard = document.createElement('script');
  guard.textContent = RUNTIME_GUARD_SCRIPT;
  document.head.appendChild(guard);
  guard.remove();
  startDevelopmentReload();
}

function clearDevelopmentStorage() {
  try {
    const namespace = document.documentElement.dataset.treeStorage || '';
    for (const storage of [localStorage, sessionStorage]) {
      for (const key of Object.keys(storage)) {
        if (key.startsWith('tree-eclass:development:') && (!namespace || !key.startsWith(namespace))) {
          storage.removeItem(key);
        }
      }
    }
  } catch {
    /* Browser privacy settings can disable storage entirely. */
  }
}
