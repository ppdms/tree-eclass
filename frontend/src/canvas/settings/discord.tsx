import { useEffect, useMemo } from 'react';
import type { SettingsDiscordChannel, SyncStatusPayload } from '@/features/settings/types';
import { formatDateTime, isSuccessfulResult } from '@/features/settings/format';
import { Notice } from '../ui/controls';
import { View } from '../ui/primitives';
import { Section } from './section';
import { FormCheck, FormField, FormSelect, SaveChanges, serializeCheckboxes, useSettingsForm } from './form';
import type { FormValues, SettingsFormModel } from './form';
import { validateExporter, validateMappings } from './validation';
import { Hint, styles, type SettingsPayload, type SettingsCourse } from './shared';

function ChannelRow({
  channel,
  courses,
  form,
}: {
  channel: SettingsDiscordChannel;
  courses: SettingsCourse[];
  form: SettingsFormModel;
}) {
  const mapped = String(channel.mapped_course_id ?? '');
  const selected = form.values[`discord_course_${channel.root_id}`] ?? mapped;
  const available = courses.filter(
    (course) => !course.hidden || String(course.id) === mapped || String(course.id) === selected,
  );
  return (
    <View style={styles.card}>
      <Hint>{`#${channel.name} · Discord channel → eClass course`}</Hint>
      <FormSelect
        form={form}
        id={`discord_course_${channel.root_id}`}
        label={`Course for #${channel.name}`}
        options={[
          { value: '', label: 'Not mapped' },
          ...available.map((course) => ({
            value: String(course.id),
            label: `${course.name}${course.hidden ? ' — hidden mapping' : ''}`,
          })),
        ]}
      />
    </View>
  );
}
function MappingForm({ data }: { data: SettingsPayload }) {
  const channels = data.discord_channels ?? [];
  const initial = useMemo(
    () =>
      Object.fromEntries(
        channels.map((channel) => [`discord_course_${channel.root_id}`, String(channel.mapped_course_id ?? '')]),
      ),
    [data.discord_channels],
  );
  const form = useSettingsForm({
    section: 'discord-course-mapping',
    endpoint: '/api/v1/settings/discord-course-map',
    initial,
    serialize: (values: FormValues) =>
      Object.fromEntries(
        channels.map((channel) => {
          const key = `discord_course_${channel.root_id}`;
          return [key, values[key] ?? String(channel.mapped_course_id ?? '')];
        }),
      ),
    validate: (values) => validateMappings(values, channels, data.courses ?? []),
  });
  const supply = form.supply;
  useEffect(() => {
    supply(initial);
  }, [initial, supply]);
  return (
    <>
      {channels.map((channel) => (
        <ChannelRow key={String(channel.root_id)} channel={channel} courses={data.courses ?? []} form={form} />
      ))}
      {!channels.length && <Hint>No Discord root channels were found.</Hint>}
      <SaveChanges form={form} />
    </>
  );
}
function ExporterForm({ data }: { data: SettingsPayload }) {
  const exporter = data.discord_exporter ?? {};
  const form = useSettingsForm({
    section: 'discord-exporter',
    endpoint: '/api/v1/settings/discord-exporter',
    initial: {
      token: '',
      interval_minutes: String(Math.round((exporter.interval_seconds || 3600) / 60)),
      parallel: String(exporter.parallel || 1),
      enabled: exporter.enabled ? 'on' : 'false',
      media: exporter.media ? 'on' : 'false',
      clear_token: 'false',
      include_threads: exporter.include_threads || 'All',
    },
    validate: validateExporter,
    serialize: serializeCheckboxes,
    nativeFields: ['token', 'interval_minutes', 'parallel'],
  });
  return (
    <>
      <FormField
        form={form}
        id="token"
        label="Discord token"
        type="password"
        autoComplete="off"
        hint={data.discord_export_token_configured ? 'Token configured — enter to replace.' : 'No token configured.'}
      />
      <FormField form={form} id="interval_minutes" label="Interval (minutes)" type="number" min={1} step={1} />
      <FormField form={form} id="parallel" label="Parallel exports" type="number" min={1} max={16} step={1} />
      <FormCheck form={form} id="enabled" label="Enable scheduled exports" />
      <FormCheck form={form} id="media" label="Include attachment media" />
      <FormCheck form={form} id="clear_token" label="Remove stored token" clear />
      <FormSelect
        form={form}
        id="include_threads"
        label="Threads"
        options={['All', 'Active', 'None'].map((value) => ({ value, label: value }))}
      />
      <SaveChanges form={form} />
    </>
  );
}
function DiscordMappingSection({ data }: { data: SettingsPayload }) {
  return (
    <Section
      id="discord-course-mapping"
      title="Discord course mapping"
      advanced
      status={`${data.discord_mapped_count || 0} of ${(data.discord_channels ?? []).length} mapped`}
      description="Associate each archive channel with the eClass course whose discussions it contains."
    >
      <MappingForm data={data} />
    </Section>
  );
}
function DiscordExporterSection({ data, sync }: { data: SettingsPayload; sync: SyncStatusPayload | null }) {
  const last = sync?.sync?.discord_export;
  const failed = Boolean(last?.last_result && !isSuccessfulResult(last.last_result));
  const status = failed
    ? 'Failed'
    : data.discord_exporter?.enabled && data.discord_export_token_configured
      ? 'Ready'
      : 'Needs setup';
  return (
    <Section
      id="discord-exporter"
      title="Discord exporter"
      advanced
      status={status}
      description="Configure the resumable scheduled archive exporter. A failed last run still needs attention."
    >
      <ExporterForm data={data} />
      {last?.last_run_at && (
        <Notice tone={isSuccessfulResult(last.last_result) ? 'success' : 'danger'}>
          {`Last export: ${formatDateTime(last.last_run_at)} · ` +
            `${isSuccessfulResult(last.last_result) ? 'succeeded' : last.last_result || 'unknown'}` +
            `${last.last_error ? ` — ${last.last_error}` : ''}`}
        </Notice>
      )}
    </Section>
  );
}
export function DiscordSections({ data, sync }: { data: SettingsPayload; sync: SyncStatusPayload | null }) {
  return (
    <>
      <DiscordMappingSection data={data} />
      <DiscordExporterSection data={data} sync={sync} />
    </>
  );
}
