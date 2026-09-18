const DISPLAY_LOCALE = 'en-US';
const DISPLAY_TIME_ZONE = 'Europe/Athens';

type DateValue = string | number | Date | null | undefined;

export function parseTimestamp(value: DateValue): Date | null {
  if (value == null || value === '') return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = raw.includes(' ') && !raw.includes('T') ? raw.replace(' ', 'T') : raw;
  const hasZone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(normalized);
  const date = new Date(hasZone ? normalized : `${normalized}Z`);
  return Number.isNaN(date.getTime()) ? null : date;
}

function calendarDay(value: DateValue): number {
  const date = parseTimestamp(value);
  if (!date) return Number.NaN;
  const parts = new Intl.DateTimeFormat(DISPLAY_LOCALE, {
    timeZone: DISPLAY_TIME_ZONE,
    year: 'numeric',
    month: 'numeric',
    day: 'numeric',
  }).formatToParts(date);
  const part = (type: Intl.DateTimeFormatPartTypes) => Number(parts.find((value) => value.type === type)?.value);
  return Date.UTC(part('year'), part('month') - 1, part('day')) / 86400000;
}

export function daysUntilCalendarDate(value: DateValue, now: DateValue = new Date()): number {
  return calendarDay(value) - calendarDay(now);
}

function formatDateValue(value: DateValue, options: Intl.DateTimeFormatOptions): string {
  const date = parseTimestamp(value);
  return date
    ? new Intl.DateTimeFormat(DISPLAY_LOCALE, {
        ...options,
        timeZone: DISPLAY_TIME_ZONE,
      }).format(date)
    : '';
}

export function formatActivityDay(value: DateValue): string {
  return formatDateValue(value, { weekday: 'long', month: 'short', day: 'numeric' });
}

export function formatTimeOfDay(value: DateValue): string {
  return formatDateValue(value, { hour: '2-digit', minute: '2-digit' });
}

export function formatDateTime(value: DateValue): string {
  return formatDateValue(value, { dateStyle: 'medium', timeStyle: 'short' });
}

export function formatCalendarDate(value: DateValue): string {
  return formatDateValue(value, { day: '2-digit', month: 'short' });
}

export function formatCalendarDay(value: DateValue): string {
  const day = formatDateValue(value, { day: 'numeric' });
  const month = formatDateValue(value, { month: 'short' });
  return day && month ? `${day} ${month}` : '';
}

export function formatShortDate(value: DateValue): string {
  return formatDateValue(value, { year: 'numeric', month: 'numeric', day: 'numeric' });
}

export function formatMonthDay(value: DateValue): string {
  return formatDateValue(value, { day: 'numeric', month: 'long' });
}

export function formatNumber(value: number): string {
  return new Intl.NumberFormat(DISPLAY_LOCALE).format(value);
}
