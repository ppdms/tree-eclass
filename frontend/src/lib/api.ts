import { z } from 'zod/v4';

/** Error thrown by fetchJson; carries the HTTP status for callers that need it. */
export class ApiError extends Error {
  override name = 'ApiError';
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

const errorBodySchema = z
  .object({
    detail: z.unknown(),
    message: z.unknown(),
  })
  .partial();

const detailItemSchema = z
  .object({
    msg: z.unknown(),
    message: z.unknown(),
  })
  .partial();

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
      const parsed = errorBodySchema.safeParse(await res.json());
      if (parsed.success) {
        const { detail, message: bodyMessage } = parsed.data;
        message = Array.isArray(detail)
          ? detail
              .map((item) => {
                const parsedItem = detailItemSchema.safeParse(item);
                return parsedItem.success
                  ? String(parsedItem.data.msg ?? parsedItem.data.message ?? item)
                  : String(item);
              })
              .join('. ')
          : String(detail ?? bodyMessage ?? message);
      }
    } catch {
      /* Keep the status fallback for non-JSON error bodies. */
    }
    throw new ApiError(message, res.status);
  }

  // SAFETY: fetchJson is a typed transport; the caller's T is the documented
  // contract for this endpoint and page code parses the payload at its boundary.
  return res.json() as Promise<T>;
}
