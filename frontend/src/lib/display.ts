// Shared display helpers for heterogeneous API values.
//
// The roadmap/insight payloads carry fields that may be a string, a number,
// an array, or a small object. These helpers render them without ad hoc
// `typeof` narrowing: scalar checks go through zod, object shapes are
// narrowed with `in` guards.

import { z } from 'zod/v4';

/** Any value a JSON payload can carry. */
export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

const scalarSchema = z.string().or(z.number());

const DISPLAY_KEYS = [
  'summary',
  'message',
  'resolution',
  'recommended_action',
  'instruction',
  'label',
  'title',
  'reason',
] as const;

/** Look up a key in a string-keyed table with a fallback. */
export function lookup<T>(table: Record<string, T>, key: string, fallback: T): T {
  const value = table[key];
  return value === undefined ? fallback : value;
}

/** Render a scalar, array, or known-shape object as display text. */
export function readableValue(value: JsonValue | undefined, fallback = 'Not provided'): string {
  if (value == null || value === '') return fallback;
  const scalar = scalarSchema.safeParse(value);
  if (scalar.success) return String(scalar.data);
  if (Array.isArray(value)) return value.map((item) => readableValue(item)).join(', ');
  if (value instanceof Object && !Array.isArray(value)) {
    for (const key of DISPLAY_KEYS) {
      if (key in value) {
        const nested = value[key];
        if (nested != null) return readableValue(nested, fallback);
      }
    }
  }
  return fallback;
}

/** True when running outside a browser (no window global). */
export function isServer(): boolean {
  return globalThis.window == null;
}

/** A structured readiness note: one of the known reason keys. */
export interface ReadinessNote {
  reason?: string;
  summary?: string;
  message?: string;
}

/** Render a readiness note: a plain string or a structured reason object. */
export function readableReason(value: JsonValue | ReadinessNote | undefined, fallback: string): string {
  if (value == null) return fallback;
  const scalar = scalarSchema.safeParse(value);
  if (scalar.success) {
    const text = String(scalar.data).trim();
    return text || fallback;
  }
  if (value instanceof Object && !Array.isArray(value)) {
    for (const key of ['reason', 'summary', 'message'] as const) {
      if (key in value) {
        const nested = value[key];
        if (nested == null) continue;
        const parsed = scalarSchema.safeParse(nested);
        if (parsed.success && String(parsed.data).trim()) return String(parsed.data);
      }
    }
  }
  return fallback;
}
