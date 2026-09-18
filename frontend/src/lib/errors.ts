// Shared error-shape helpers.
//
// Caught values are parsed at the catch site with `errorLikeSchema`; the
// parsed `ErrorLike` is what flows into these helpers and the UI.

import { z } from 'zod/v4';

/** The error shape the app understands: an HTTP status and/or a message. */
export interface ErrorLike {
  status?: number;
  message?: unknown;
}

export const errorLikeSchema = z
  .object({
    status: z.number().optional(),
    message: z.unknown().optional(),
  })
  .partial();

/** The message of a parsed error, or the fallback when it is absent/blank. */
export function errorMessage(error: ErrorLike, fallback: string): string {
  const message = z.string().safeParse(error.message);
  return message.success && message.data.trim() ? message.data : fallback;
}

/** True when the parsed error is a 404 (status or message text). */
export function isNotFoundError(error: ErrorLike): boolean {
  if (error.status === 404) return true;
  return /404|not found/i.test(errorMessage(error, ''));
}
