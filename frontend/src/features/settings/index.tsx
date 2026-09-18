import { SyncStatusProvider } from './syncStatus';

import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import { fetchJson } from '@/lib/api';
import { lookup } from '@/lib/display';
import type { SettingsPayload } from './types';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import { DIRTY_FORMS, ACTION_TO_SECTION } from './state';
import { Section } from './settingsForm';
import { ThemePicker } from './themePicker';
import { SettingsNav } from './settingsNav';
import { CheckStatus } from './checkStatus';
import { KnowledgeMaintenance } from './knowledge';
import { CourseSections } from './courseSections';
import { DiscordSections } from './discordSections';
import { IntegrationSections } from './integrationSections';
import { Preferences, DataExport } from './preferences';
import { AIBackendSettings } from './aiSettings';

const styles = stylex.create({
  settingsGroup: {
    marginBottom: '1.5rem',
    minWidth: 0,
  },
  settingsGroupDaily: {},
  settingsGroupAdvanced: {},

  settingsGroupTitle: {
    marginInline: 0,
    color: colors.textPrimary,
    fontSize: '1.125rem',
    fontWeight: typography.weightSemibold,
    letterSpacing: '-0.025em',
    marginBlockEnd: '0.75rem',
    marginBlockStart: 0,
  },
  settingsPage: {
    gap: '1rem',
    display: 'flex',
    flexDirection: 'column',
    minWidth: 0,
    width: '100%',
  },
  settingsLayout: {
    gap: '2rem',
    marginInline: 'auto',
    alignItems: 'start',
    display: 'grid',
    gridTemplateColumns: {
      default: '14.5rem minmax(0, 1fr)',
      [media.tablet]: 'minmax(0, 1fr)',
    },
    maxWidth: '73.75rem',
    minWidth: 0,
    width: '100%',
  },
  settingsPageHeader: {
    marginBottom: '0.5rem',
  },
  pageTitle: {
    margin: 0,
    color: colors.textPrimary,
    fontSize: '1.5rem',
    fontWeight: typography.weightSemibold,
    letterSpacing: '-0.03em',
    lineHeight: 1.15,
  },
});

interface SectionsProps {
  data: SettingsPayload;
  dirtySections: Set<string>;
}

function DailySections({ data, dirtySections }: SectionsProps) {
  return (
    <div {...stylex.props(styles.settingsGroup, styles.settingsGroupDaily)}>
      <h2 {...stylex.props(styles.settingsGroupTitle)}>Daily workspace</h2>
      <Section
        id="appearance"
        title="Appearance"
        status="Stored on this device"
        description={
          'Follow the system look, or choose light, dark, or e-ink for this browser. ' +
          'E-ink is a reading mode, not a competing theme.'
        }
      >
        <ThemePicker />
      </Section>
      <CourseSections data={data} dirtySections={dirtySections} />
    </div>
  );
}

function AdvancedSections({ data, dirtySections }: SectionsProps) {
  return (
    <div {...stylex.props(styles.settingsGroup, styles.settingsGroupAdvanced)}>
      <h2 {...stylex.props(styles.settingsGroupTitle)}>Advanced integrations</h2>
      <KnowledgeMaintenance courses={data.courses || []} />
      <AIBackendSettings data={data} dirtySections={dirtySections} />
      <DiscordSections data={data} dirtySections={dirtySections} />
      <IntegrationSections data={data} dirtySections={dirtySections} />
      <Preferences data={data} dirtySections={dirtySections} />
      <DataExport />
    </div>
  );
}

async function fetchSettingsWithTimeout(): Promise<SettingsPayload> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 8000);
  try {
    return await fetchJson<SettingsPayload>('api/v1/settings', { signal: controller.signal });
  } catch (loadError) {
    if (loadError instanceof Error && loadError.name === 'AbortError')
      throw new Error('Settings took too long to load.');
    throw loadError;
  } finally {
    window.clearTimeout(timeout);
  }
}

function useSettingsData(initialData: SettingsPayload | null) {
  const [data, setData] = React.useState<SettingsPayload | null>(initialData);
  const [loading, setLoading] = React.useState(!initialData);
  const [error, setError] = React.useState<string | null>(null);
  const loadSettings = React.useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setData(await fetchSettingsWithTimeout());
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : 'Network connection issue');
    } finally {
      setLoading(false);
    }
  }, []);
  React.useEffect(() => {
    if (initialData) {
      setData(initialData);
      setLoading(false);
      return;
    }
    loadSettings();
  }, [initialData, loadSettings]);
  React.useEffect(() => {
    window.addEventListener('settings-saved', loadSettings);
    return () => window.removeEventListener('settings-saved', loadSettings);
  }, [loadSettings]);
  return { data, loading, error, retry: loadSettings };
}

export interface SettingsPageProps {
  initialData?: SettingsPayload | null;
}

function useDirtySections(): Set<string> {
  const [dirtySections, setDirtySections] = React.useState<Set<string>>(new Set());
  React.useEffect(() => {
    const update = () =>
      setDirtySections(
        new Set<string>([...DIRTY_FORMS].map((action) => lookup(ACTION_TO_SECTION, action, '')).filter(Boolean)),
      );
    update();
    window.addEventListener('settings-dirty-change', update);
    return () => window.removeEventListener('settings-dirty-change', update);
  }, []);
  return dirtySections;
}

function SettingsPageError({ error, retry, loading }: { error: string; retry: () => void; loading: boolean }) {
  return (
    <div {...stylex.props(styles.settingsPage)}>
      <p role="alert">Could not load settings ({error}).</p>
      <button
        type="button"
        {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
        onClick={retry}
        disabled={loading}
      >
        {loading ? 'Trying again…' : 'Try again'}
      </button>
    </div>
  );
}

function SettingsPageLoading() {
  return (
    <div {...stylex.props(styles.settingsPage)}>
      <p role="status" aria-live="polite">
        Opening settings…
      </p>
    </div>
  );
}

function SettingsPageBody({ data, dirtySections }: { data: SettingsPayload; dirtySections: Set<string> }) {
  return (
    <div {...stylex.props(styles.settingsLayout)}>
      <SettingsNav dirtySections={dirtySections} />
      <div {...stylex.props(styles.settingsPage)}>
        <header {...stylex.props(styles.settingsPageHeader)}>
          <h1 {...stylex.props(styles.pageTitle)}>Settings</h1>
          <CheckStatus />
        </header>
        <DailySections data={data} dirtySections={dirtySections} />
        <AdvancedSections data={data} dirtySections={dirtySections} />
      </div>
    </div>
  );
}

export default function SettingsPage({ initialData = null }: SettingsPageProps) {
  const { data, loading, error, retry } = useSettingsData(initialData);
  const dirtySections = useDirtySections();
  if (error && !data) {
    return <SettingsPageError error={error} retry={retry} loading={loading} />;
  }
  if (!data) return <SettingsPageLoading />;
  return (
    <SyncStatusProvider>
      <SettingsPageBody data={data} dirtySections={dirtySections} />
    </SyncStatusProvider>
  );
}
