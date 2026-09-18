import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import type { CourseSummary } from '@/lib/types';
import type { SettingsDiscordChannel, SettingsPayload } from './types';
import { media } from '@/styles/constants.stylex';
import { colors, layout, spacing, typography } from '@/styles/tokens.stylex';
import { Section, SettingsForm } from './settingsForm';
import { Field, ClearSecret } from './fields';
import { LastRunLine, useSyncStatus } from './syncStatus';
import { isSuccessfulResult } from './format';

const styles = stylex.create({
  mapLabel: {
    margin: 0,
    gap: spacing.xs,
    display: 'flex',
    flexDirection: 'column',
    minWidth: 0,
  },
  channelName: {
    gap: spacing.sm,
    alignItems: 'center',
    display: 'flex',
    fontWeight: typography.weightSemibold,
    overflowWrap: 'anywhere',
  },
  accentIcon: {
    color: colors.statusAccent,
  },
  subLabel: {
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
  },
  select: {
    borderColor: {
      default: colors.border,
      ':hover:not(:disabled)': colors.borderLight,
      ':focus': colors.focusRing,
    },
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    outline: {
      default: 'none',
      ':focus': 'none',
    },
    paddingBlock: 0,
    paddingInline: '0.75rem',
    backgroundColor: colors.surface,
    boxShadow: {
      default: 'none',
      ':focus': `0 0 0 0.1875rem color-mix(in srgb, ${colors.focusRing} 28%, transparent)`,
    },
    color: colors.textPrimary,
    cursor: { default: 'auto', ':disabled': 'not-allowed' },
    fontSize: typography.sizeBase,
    opacity: { default: 1, ':disabled': 0.65 },
    transitionDuration: '150ms',
    transitionProperty: 'border-color, box-shadow, background-color',
    height: 'auto',
    minHeight: '2.5rem',
    minWidth: 0,
    width: '100%',
  },
  discordMapRow: {
    gap: {
      default: spacing.md,
      [media.tablet]: spacing.xs,
    },
    paddingBlock: '0.75rem',
    alignItems: 'center',
    display: 'grid',
    gridTemplateColumns: {
      default: 'minmax(0, 0.9fr) auto minmax(0, 1.1fr)',
      [media.tablet]: 'minmax(0, 1fr)',
    },
    borderBottomColor: colors.border,
    borderBottomStyle: 'solid',
    borderBottomWidth: {
      default: 1,
      ':last-child': 0,
    },
  },
  discordMapArrow: {
    color: colors.textSecondary,
  },
  channelList: {
    gap: 0,
    display: 'flex',
    flexDirection: 'column',
  },
  settingsSectionDescription: {
    marginBlock: '0.35rem',
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
  },
  settingsMatrix: {
    gap: '1rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: 'minmax(0, 1fr)',
    },
  },
  checkRow: {
    gap: '0.5rem',
    alignItems: 'center',
    display: 'flex',
    minHeight: '2.5rem',
  },
  formGroup: {
    gap: '0.35rem',
    display: 'flex',
    flexDirection: 'column',
    marginBottom: '1rem',
  },
});

export interface DiscordMapRowProps {
  channel: SettingsDiscordChannel;
  courses: CourseSummary[];
}

function DiscordMapLabel({ channel }: { channel: SettingsDiscordChannel }) {
  return (
    <label htmlFor={`discord_course_${channel.root_id}`} {...stylex.props(styles.mapLabel)}>
      <span {...stylex.props(styles.channelName)}>
        <span {...stylex.props(styles.accentIcon)}>
          <Icon name="discord" aria-hidden="true" />
        </span>{' '}
        #{channel.name}
      </span>
      <span {...stylex.props(styles.subLabel)}>Discord channel</span>
    </label>
  );
}

function DiscordMapSelect({ channel, available }: { channel: SettingsDiscordChannel; available: CourseSummary[] }) {
  return (
    <select
      id={`discord_course_${channel.root_id}`}
      name={`discord_course_${channel.root_id}`}
      defaultValue={channel.mapped_course_id ? String(channel.mapped_course_id) : ''}
      {...stylex.props(styles.select)}
    >
      <option value="">Not mapped</option>
      {available.map((course) => (
        <option value={course.id} key={course.id}>
          {course.name}
          {course.hidden ? ' — hidden mapping' : ''}
        </option>
      ))}
    </select>
  );
}

export function DiscordMapRow({ channel, courses }: DiscordMapRowProps) {
  const mapped = courses.find((course) => String(course.id) === String(channel.mapped_course_id));
  const available = courses.filter((course) => !course.hidden || course.id === mapped?.id);
  return (
    <div {...stylex.props(styles.discordMapRow)}>
      <DiscordMapLabel channel={channel} />
      <span {...stylex.props(styles.discordMapArrow)}>
        <Icon name="arrow-right" aria-hidden="true" />
      </span>
      <DiscordMapSelect channel={channel} available={available} />
    </div>
  );
}

export interface DiscordCourseMappingSectionProps {
  data: SettingsPayload;
  dirtySections: Set<string>;
}

export function DiscordCourseMappingSection({ data, dirtySections }: DiscordCourseMappingSectionProps) {
  return (
    <Section
      id="discord-course-mapping"
      title="Discord course mapping"
      collapsible
      defaultOpen={false}
      dirty={dirtySections.has('discord-course-mapping')}
      status={`${data.discord_mapped_count || 0} of ${(data.discord_channels || []).length} mapped`}
      description="Associate each archive channel with the eClass course whose discussions it contains."
    >
      <SettingsForm action="/settings/discord-course-map">
        <div {...stylex.props(styles.channelList)}>
          {(data.discord_channels || []).map((channel) => (
            <DiscordMapRow channel={channel} courses={data.courses || []} key={channel.root_id} />
          ))}
        </div>
        {!data.discord_channels?.length && (
          <p {...stylex.props(styles.settingsSectionDescription)}>No Discord root channels were found.</p>
        )}
      </SettingsForm>
    </Section>
  );
}

interface DiscordExporter {
  enabled?: boolean;
  media?: boolean;
  interval_seconds?: number;
  parallel?: number;
  include_threads?: string;
}

export interface ExporterFieldsProps {
  exporter: DiscordExporter;
  tokenConfigured?: boolean;
}

export function ExporterFields({ exporter, tokenConfigured }: ExporterFieldsProps) {
  return (
    <div {...stylex.props(styles.settingsMatrix)}>
      <Field
        label="Discord token"
        name="token"
        type="password"
        hint={tokenConfigured ? 'Token configured — enter to replace.' : 'No token configured.'}
      />
      <Field
        label="Interval (minutes)"
        name="interval_minutes"
        type="number"
        min="1"
        defaultValue={Math.round((exporter.interval_seconds || 3600) / 60)}
      />
      <Field
        label="Parallel exports"
        name="parallel"
        type="number"
        min="1"
        max="16"
        defaultValue={exporter.parallel || 1}
      />
      <label {...stylex.props(styles.checkRow)}>
        <input type="checkbox" name="enabled" defaultChecked={exporter.enabled} /> Enable scheduled exports
      </label>
      <label {...stylex.props(styles.checkRow)}>
        <input type="checkbox" name="media" defaultChecked={exporter.media} /> Include attachment media
      </label>
      <ClearSecret name="clear_token" label="Remove stored token" />
    </div>
  );
}

export interface ExporterThreadsProps {
  exporter: DiscordExporter;
}

export function ExporterThreads({ exporter }: ExporterThreadsProps) {
  return (
    <label {...stylex.props(styles.formGroup)}>
      <span>Threads</span>
      <select name="include_threads" defaultValue={exporter.include_threads || 'All'}>
        <option>All</option>
        <option>Active</option>
        <option>None</option>
      </select>
    </label>
  );
}

export interface DiscordExporterSectionProps {
  data: SettingsPayload;
  dirtySections: Set<string>;
}

export function DiscordExporterSection({ data, dirtySections }: DiscordExporterSectionProps) {
  const exporter: DiscordExporter = data.discord_exporter || {};
  const lastRun = useSyncStatus()?.sync?.discord_export || null;
  const failed = Boolean(lastRun?.last_result && !isSuccessfulResult(lastRun.last_result));
  const status = failed ? 'Failed' : exporter.enabled && data.discord_export_token_configured ? 'Ready' : 'Needs setup';
  return (
    <Section
      id="discord-exporter"
      title="Discord exporter"
      collapsible
      defaultOpen={false}
      dirty={dirtySections.has('discord-exporter')}
      status={status}
      description={
        'Configure the resumable archive exporter. A failed last run needs attention ' +
        'even when the exporter is configured.'
      }
    >
      <SettingsForm action="/settings/discord-exporter">
        <ExporterFields exporter={exporter} tokenConfigured={data.discord_export_token_configured} />
        <ExporterThreads exporter={exporter} />
      </SettingsForm>
      <LastRunLine job="discord_export" label="export" />
    </Section>
  );
}

export interface DiscordSectionsProps {
  data: SettingsPayload;
  dirtySections: Set<string>;
}

export function DiscordSections({ data, dirtySections }: DiscordSectionsProps) {
  return (
    <>
      <DiscordCourseMappingSection data={data} dirtySections={dirtySections} />
      <DiscordExporterSection data={data} dirtySections={dirtySections} />
    </>
  );
}
