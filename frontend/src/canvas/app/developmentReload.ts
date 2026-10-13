/** A source rebuild can reload only a page from this original development runtime.
 * Runtime conflicts permanently stop polling; they never refresh stale authority. */
export function startDevelopmentReload(): void {
  const { treeMode, treeRuntime, treeBuild } = document.documentElement.dataset;
  if (treeMode !== 'development') return;
  let polling = false;
  let stale = false;
  const stop = () => {
    stale = true;
    window.clearInterval(timer);
  };
  const timer = window.setInterval(async () => {
    if (stale || polling || document.hidden) return;
    polling = true;
    try {
      const response = await fetch('/_app/build', {
        headers: { 'X-Tree-Runtime': treeRuntime || '' },
        cache: 'no-store',
      });
      if (response.status === 409) {
        stop();
        window.dispatchEvent(new Event('tree-runtime-changed'));
      } else if (response.ok) {
        const build = await response.text();
        if (!stale && build !== treeBuild) window.location.reload();
      }
    } catch {
      // A Go rebuild can briefly stop HTTP; the next tick retries.
    } finally {
      polling = false;
    }
  }, 1000);
  window.addEventListener('tree-runtime-changed', stop);
}
