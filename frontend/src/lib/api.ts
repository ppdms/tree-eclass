import type { JsonValue } from './display';

/** Error thrown by fetchJson; carries the HTTP status for callers that need it. */
export class ApiError extends Error {
  override name = 'ApiError';
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

function isErrorObject(value: JsonValue): value is { [key: string]: JsonValue } {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function detailText(item: JsonValue): string {
  if (!isErrorObject(item)) return String(item);
  const msg = 'msg' in item ? item.msg : undefined;
  const message = 'message' in item ? item.message : undefined;
  return String(msg ?? message ?? item);
}

function errorText(body: JsonValue, fallback: string): string {
  if (!isErrorObject(body)) return fallback;
  const detail = 'detail' in body ? body.detail : undefined;
  const message = 'message' in body ? body.message : undefined;
  return Array.isArray(detail) ? detail.map(detailText).join('. ') : String(detail ?? message ?? fallback);
}

export type FetchJsonOptions = Omit<RequestInit, 'cache'>;

export async function fetchJson<T>(path: string, options: FetchJsonOptions = {}): Promise<T> {
  const targetUrl = path.startsWith('/') ? path : `/${path}`;
  const res = await fetch(targetUrl, {
    ...options,
    cache: 'no-store',
    credentials: options.credentials || 'same-origin',
    headers: {
      Accept: 'application/json',
      ...options.headers,
    },
  });

  if (!res.ok) {
    let message = `Server returned HTTP ${res.status}`;
    try {
      message = errorText(await res.json(), message);
    } catch {
      /* Keep the status fallback for non-JSON error bodies. */
    }
    throw new ApiError(message, res.status);
  }

  // SAFETY: fetchJson is a typed transport; the caller's T is the documented
  // contract for this endpoint and page code parses the payload at its boundary.
  return res.json() as Promise<T>;
}
