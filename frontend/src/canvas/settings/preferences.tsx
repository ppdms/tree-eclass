import { usePreference } from '../app/preferences';
import type { JsonValue } from '@/lib/display';
import { AppLink } from '../app/links';
import { Button } from '../ui/controls';
import { Icon, Text, View } from '../ui/primitives';
import { colors, type ThemeName } from '../ui/palette';
import { Section } from './section';
import { FormCheck, FormField, SaveChanges, useSettingsForm } from './form';
import { validatePreferences } from './validation';
import { styles, type SettingsPayload } from './shared';

interface ThemeChoice {
  value: ThemeName;
  label: string;
  swatch: string[];
}
const themes: ThemeChoice[] = [
  { value: 'system', label: 'System', swatch: ['#fafafa', '#09090b', '#a1a1aa'] },
  { value: 'light', label: 'Light', swatch: ['#fafafa', '#ffffff', '#18181b'] },
  { value: 'dark', label: 'Dark', swatch: ['#09090b', '#111113', '#fafafa'] },
  { value: 'eink', label: 'E-ink', swatch: ['#ffffff', '#ffffff', '#000000'] },
];
function ThemeOption({ option, selected, onSelect }: { option: ThemeChoice; selected: boolean; onSelect(): void }) {
  return (
    <Button
      id={`settings-theme-${option.value}`}
      pressed={selected}
      onClick={onSelect}
      style={{
        flexGrow: 1,
        flexDirection: 'column',
        padding: 14,
        borderColor: selected ? colors.accent : colors.border,
      }}
    >
      <View style={{ flexDirection: 'row', width: 92, height: 28, borderWidth: 1, borderColor: colors.border }}>
        {option.swatch.map((color, band) => (
          <View key={`${option.value}-${band}`} style={{ backgroundColor: color, width: 30, height: 26 }} />
        ))}
      </View>
      <View style={styles.row}>
        <Text>{option.label}</Text>
        {selected && <Icon name="check" style={{ width: 16, height: 16, color: colors.accent }} />}
      </View>
    </Button>
  );
}
export function AppearanceSection() {
  const [stored, setTheme] = usePreference('treeEclass.theme', 'system', parseStoredTheme);
  const theme = themes.some((item) => item.value === stored) ? stored : 'system';
  return (
    <Section
      id="appearance"
      title="Appearance"
      status="Stored on this device"
      description="Follow the system look, or choose light, dark, or e-ink for this browser. E-ink is a reading mode."
    >
      <View style={styles.row}>
        {themes.map((option) => (
          <ThemeOption
            key={option.value}
            option={option}
            selected={theme === option.value}
            onSelect={() => setTheme(option.value)}
          />
        ))}
      </View>
    </Section>
  );
}
function isStoredTheme(value: JsonValue): value is string {
  return typeof value === 'string';
}
function parseStoredTheme(value: JsonValue): string {
  return isStoredTheme(value) ? value : 'system';
}
const feedOptions = [
  ['global_feed_dept_enabled', 'Show department announcements in Activity'],
  ['global_feed_undergrad_enabled', 'Show undergraduate announcements in Activity'],
  ['global_feed_rector_enabled', 'Show rector announcements in Activity'],
] as const;
function PreferencesForm({ data }: { data: SettingsPayload }) {
  const prefs = data.preferences ?? {};
  const form = useSettingsForm({
    section: 'preferences',
    endpoint: '/api/v1/settings/preferences',
    validate: validatePreferences,
    nativeFields: ['check_interval_minutes', 'download_base_path'],
    initial: {
      check_interval_minutes: String(prefs.check_interval_minutes || 60),
      download_base_path: prefs.download_base_path || '',
      global_feed_dept_enabled: prefs.global_feed_dept_enabled ? 'on' : 'false',
      global_feed_undergrad_enabled: prefs.global_feed_undergrad_enabled ? 'on' : 'false',
      global_feed_rector_enabled: prefs.global_feed_rector_enabled ? 'on' : 'false',
    },
  });
  return (
    <>
      <FormField
        form={form}
        id="check_interval_minutes"
        label="Check interval (minutes)"
        type="number"
        min={5}
        max={1440}
        step={1}
      />
      <FormField
        form={form}
        id="download_base_path"
        label="Local mirror path"
        placeholder="/University"
        hint="Course checks refresh this folder under your home directory. Leave blank to disable mirroring."
      />
      {feedOptions.map(([id, label]) => (
        <FormCheck key={id} form={form} id={id} label={label} />
      ))}
      <SaveChanges form={form} />
    </>
  );
}
export function PreferencesSection({ data }: { data: SettingsPayload }) {
  return (
    <Section
      id="preferences"
      title="Application preferences"
      advanced
      description="Control checker timing, the automatic course mirror, and global announcement feeds."
    >
      <PreferencesForm data={data} />
    </Section>
  );
}
export function DataExportSection() {
  return (
    <Section
      id="data-export"
      title="Learner data"
      advanced
      description="Download a portable copy of annotations, notes, the study plan, and study history."
    >
      <AppLink
        id="settings-download-learner-data"
        href="/api/settings/export"
        download="tree-eclass-learner-data.json"
        block
        style={{ ...styles.card, backgroundColor: colors.raised }}
      >
        <Text>Download learner data</Text>
      </AppLink>
    </Section>
  );
}
