/** Runs before React mounts, so SDK transports carry the generation of the
 * page that created them. All mutations go through fetch with the
 * X-Tree-Runtime header; native form posts no longer exist. Never read a
 * refreshed cookie. */
export const RUNTIME_GUARD_SCRIPT = `
(() => {
  const session = document.documentElement.dataset.treeRuntime;
  if (!session) return;
  const origin = window.location.origin;
  const originalFetch = window.fetch.bind(window);
  window.fetch = async (input, init) => {
    const request = input instanceof Request ? input : null;
    const url = new URL(request ? request.url : String(input), window.location.href);
    const method = String(init?.method || request?.method || 'GET').toUpperCase();
    const mutation = url.origin === origin && !['GET', 'HEAD', 'OPTIONS'].includes(method);
    let options = init;
    if (mutation) {
      const headers = new Headers(init?.headers || request?.headers);
      headers.set('X-Tree-Runtime', session);
      options = { ...init, headers };
    }
    const response = await originalFetch(input, options);
    if (mutation && response.status === 409) {
      void response.clone().json().then((body) => {
        if (body.code === 'runtime_changed') window.dispatchEvent(new Event('tree-runtime-changed'));
      }).catch(() => {});
    }
    return response;
  };
})();
`;
