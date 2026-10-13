import { Button, Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { SettingsContext, styles, type SettingsPayload } from './shared';
import { CheckStatusHeader } from './check';
import { AppearanceSection, DataExportSection, PreferencesSection } from './preferences';
import { CourseSections } from './courses';
import { KnowledgeSection } from './knowledge';
import { AISection } from './ai';
import { DiscordSections } from './discord';
import { IntegrationSections } from './integrations';
import { SettingsNavigation, useSettingsWorkspace } from './navigation';
import { useSettingsData, type SettingsLoad } from './data';
import { useSyncStatus } from './resources';
import type { SyncStatusPayload } from '@/features/settings/types';

function DailySettings({ data }: { data: SettingsPayload }) {
  return (
    <View style={styles.stack}>
      <Text style={styles.heading}>Daily workspace</Text>
      <AppearanceSection />
      <CourseSections data={data} />
    </View>
  );
}
function AdvancedSettings({ data, sync }: { data: SettingsPayload; sync: SyncStatusPayload | null }) {
  return (
    <View style={styles.stack}>
      <Text style={styles.heading}>Advanced integrations</Text>
      <KnowledgeSection courses={data.courses ?? []} />
      <AISection data={data} />
      <DiscordSections data={data} sync={sync} />
      <IntegrationSections data={data} sync={sync} />
      <PreferencesSection data={data} />
      <DataExportSection />
    </View>
  );
}
export function SettingsPage({ initial }: { initial: SettingsLoad }) {
  const resource = useSettingsData(initial);
  const workspace = useSettingsWorkspace(resource.refresh);
  const sync = useSyncStatus();
  return (
    <SettingsContext.Provider value={workspace.context}>
      <View
        id="settings-page"
        ready
        style={{ ...styles.page, flexDirection: workspace.viewport.width > 900 ? 'row' : 'column' }}
      >
        <SettingsNavigation active={workspace.active} />
        <View id="settings-content" style={{ ...styles.stack, width: undefined, flexGrow: 1, flexShrink: 1 }}>
          <Text style={styles.title}>Settings</Text>
          <CheckStatusHeader />
          {resource.error && <Notice tone="warning">{`Could not refresh settings: ${resource.error}`}</Notice>}
          {resource.error && (
            <Button id="settings-refresh-retry" disabled={resource.loading} onClick={resource.refresh}>
              {resource.loading ? 'Trying again…' : 'Try again'}
            </Button>
          )}
          <DailySettings data={resource.data} />
          <AdvancedSettings data={resource.data} sync={sync} />
        </View>
      </View>
    </SettingsContext.Provider>
  );
}
