import type { FormValues, Validation } from './form';
import type { SettingsDiscordChannel } from '@/features/settings/types';
import type { SettingsCourse } from './shared';

export function integerField(values: FormValues, key: string, min: number, max: number): Validation {
  const raw = values[key]?.trim() ?? '';
  const value = Number(raw);
  if (!/^[+-]?\d+$/.test(raw) || !Number.isSafeInteger(value) || value < min || value > max) {
    return { field: key, message: `${key} must be a whole number between ${min} and ${max}.` };
  }
  return null;
}
export function validateWebhook(values: FormValues): Validation {
  const raw = values.webhook_url?.trim() ?? '';
  if (values.clear_webhook === 'true' || !raw) return null;
  const invalid = {
    field: 'webhook_url',
    message: 'Webhook must be an absolute HTTP or HTTPS URL without user information or fragment.',
  };
  try {
    const url = new URL(raw);
    if (
      !/^https?:\/\//i.test(raw) ||
      !url.hostname ||
      url.username ||
      url.password ||
      url.hash ||
      new TextEncoder().encode(raw).length > 4096
    )
      return invalid;
  } catch {
    return invalid;
  }
  return null;
}
export function validatePreferences(values: FormValues): Validation {
  const interval = integerField(values, 'check_interval_minutes', 5, 1440);
  if (interval) return interval;
  const raw = values.download_base_path?.trim() ?? '';
  if (!raw) return null;
  const parts = raw.normalize('NFC').split('/').filter(Boolean);
  if (!parts.length)
    return { field: 'download_base_path', message: 'Download base path must be a non-root local path.' };
  if (parts.some((part) => part === '.' || part === '..')) {
    return { field: 'download_base_path', message: 'Download base path must not contain dot segments.' };
  }
  return null;
}
export function validateExporter(values: FormValues): Validation {
  const numberError = integerField(values, 'interval_minutes', 1, 10080) ?? integerField(values, 'parallel', 1, 16);
  if (numberError) return numberError;
  if (!['All', 'Active', 'None'].includes(values.include_threads ?? '')) {
    return { field: 'include_threads', message: 'Thread policy must be None, Active, or All.' };
  }
  return null;
}

export function validateMappings(
  values: FormValues,
  channels: SettingsDiscordChannel[],
  courses: SettingsCourse[],
): Validation {
  for (const channel of channels) {
    const field = `discord_course_${channel.root_id}`;
    const value = values[field] ?? String(channel.mapped_course_id ?? '');
    if (value && !courses.some((course) => String(course.id) === value)) {
      return { field, message: 'Unknown eClass course selection.' };
    }
  }
  return null;
}
