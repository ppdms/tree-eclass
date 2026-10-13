import { fetchJson } from '@/lib/api';
import type { JsonValue } from '@/lib/display';

let configured = false;

/** Set once from the original page; never refresh this from a cookie or API response. */
export function configureAPI(runtime: string, changed: () => void): void {
  if (configured) throw new Error('The canvas API runtime was already captured');
  configured = true;
  const original = globalThis.fetch.bind(globalThis);
  const origin = self.location.origin;
  globalThis.fetch = async (input, init) => {
    const request = input instanceof Request ? input : null;
    const url = new URL(request ? request.url : String(input), self.location.href);
    const method = String(init?.method ?? request?.method ?? 'GET').toUpperCase();
    const mutation = url.origin === origin && !['GET', 'HEAD', 'OPTIONS'].includes(method);
    let options = init;
    if (mutation) {
      const headers = new Headers(init?.headers ?? request?.headers);
      headers.set('X-Tree-Runtime', runtime);
      options = { ...init, headers };
    }
    const response = await original(input, options);
    if (mutation && response.status === 409) await detectRuntimeChange(response, changed);
    return response;
  };
}

function isRuntimeChange(body: JsonValue): body is { code: 'runtime_changed' } {
  return (
    body !== null &&
    typeof body === 'object' &&
    !Array.isArray(body) &&
    'code' in body &&
    body.code === 'runtime_changed'
  );
}

async function detectRuntimeChange(response: Response, changed: () => void): Promise<void> {
  try {
    if (isRuntimeChange(await response.clone().json())) changed();
  } catch {
    // Non-JSON conflicts keep their original body and normal caller error handling.
  }
}

/** Streaming and binary responses use the same captured-runtime fetch boundary. */
export function request(path: string, options: RequestInit = {}): Promise<Response> {
  return fetch(path, { cache: 'no-store', credentials: 'same-origin', ...options });
}

export function requestJson<T>(path: string, options: RequestInit = {}): Promise<T> {
  if (options.headers instanceof Headers || Array.isArray(options.headers)) {
    const headers = Object.fromEntries(new Headers(options.headers).entries());
    return fetchJson<T>(path, { ...options, headers });
  }
  return fetchJson<T>(path, options);
}
