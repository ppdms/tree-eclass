/** Only a successful source rebuild in this same development session reloads the
 * page. A changed runtime never upgrades the authority of an already-open tab. */
export function startDevelopmentReload() {
  const { treeMode, treeRuntime, treeBuild } = document.documentElement.dataset;
  if (treeMode !== 'development') return;
  let polling = false;
  const timer = window.setInterval(async () => {
    if (polling || document.hidden) return;
    polling = true;
    try {
      const response = await fetch('/_app/build', {
        headers: { 'X-Tree-Runtime': treeRuntime || '' },
        cache: 'no-store',
      });
      if (response.status === 409) {
        window.clearInterval(timer);
        window.dispatchEvent(new Event('tree-runtime-changed'));
      } else if (response.ok && (await response.text()) !== treeBuild) {
        window.location.reload();
      }
    } catch {
      // A Go rebuild can briefly stop HTTP; the next tick retries.
    } finally {
      polling = false;
    }
  }, 1000);
}
