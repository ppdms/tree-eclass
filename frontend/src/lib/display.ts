// Shared display helpers for heterogeneous API values.
//
// The roadmap/insight payloads carry fields that may be a string, a number,
// an array, or a small object. Centralize scalar checks here; object shapes
// are narrowed with `in` guards.

/** Any value a JSON payload can carry. */
export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

function isScalar(value: JsonValue | ReadinessNote): value is string | number {
  return typeof value === 'string' || (typeof value === 'number' && Number.isFinite(value));
}

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
  if (isScalar(value)) return String(value);
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

/** A structured readiness note: one of the known reason keys. */
export interface ReadinessNote {
  reason?: string;
  summary?: string;
  message?: string;
}

/** Render a readiness note: a plain string or a structured reason object. */
export function readableReason(value: JsonValue | ReadinessNote | undefined, fallback: string): string {
  if (value == null) return fallback;
  if (isScalar(value)) {
    const text = String(value).trim();
    return text || fallback;
  }
  if (value instanceof Object && !Array.isArray(value)) {
    for (const key of ['reason', 'summary', 'message'] as const) {
      if (key in value) {
        const nested = value[key];
        if (nested == null) continue;
        if (isScalar(nested) && String(nested).trim()) return String(nested);
      }
    }
  }
  return fallback;
}
