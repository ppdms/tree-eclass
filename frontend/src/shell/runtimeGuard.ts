/** Runs before React mounts, so SDK transports and early form submissions carry
 * the generation of the page that created them. Never read a refreshed cookie. */
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
  document.addEventListener('submit', (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    const submitter = event.submitter;
    const action = submitter?.hasAttribute('formaction') ? submitter.formAction : form.action;
    const method = (submitter?.hasAttribute('formmethod') ? submitter.formMethod : form.method).toLowerCase();
    if (method !== 'post' || new URL(action, window.location.href).origin !== origin) return;
    let field = form.querySelector('input[name="_tree_runtime"]');
    if (!field) {
      field = document.createElement('input');
      field.type = 'hidden';
      field.name = '_tree_runtime';
      form.appendChild(field);
    }
    field.value = session;
  }, true);
})();
`;
