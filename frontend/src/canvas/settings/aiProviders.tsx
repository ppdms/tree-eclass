import type { AISettings } from '@/features/settings/types';
import { lookup } from '@/lib/display';
import { Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { FormCheck, FormField, FormSelect, type SettingsFormModel } from './form';
import { providers, providerOptions, type Provider } from './aiModel';
import { Group, Hint, styles } from './shared';

const statusLabels = {
  available: 'quota tracked · available',
  blocked: 'quota paused',
  limited: 'quota paused',
  unchecked: 'quota not checked yet',
  disabled: 'disabled',
  ai_disabled: 'AI analysis off',
} satisfies Record<string, string>;
function ProviderCard({
  provider,
  settings,
  form,
}: {
  provider: Provider;
  settings: AISettings;
  form: SettingsFormModel;
}) {
  const enabled = form.values[`provider_enabled_${provider.id}`] === 'on';
  const status = settings.provider_status?.[provider.id]?.status;
  const statusLabel =
    !status || status === 'unknown' ? 'worker has not reported' : lookup(statusLabels, status, status);
  return (
    <Panel id={`settings-provider-${provider.id}`}>
      <FormCheck form={form} id={`provider_enabled_${provider.id}`} label={provider.label} />
      <Hint>{`${settings.provider_credentials?.[provider.id] ? 'Key configured' : 'No key'} · ${statusLabel}`}</Hint>
      {enabled ? (
        <View style={styles.stack}>
          {provider.chat && (
            <FormField
              form={form}
              id={provider.chat}
              required
              label={provider.chat === provider.analysis ? 'Model (Ask + analysis)' : 'Ask model'}
            />
          )}
          {provider.analysis && provider.analysis !== provider.chat && (
            <FormField form={form} id={provider.analysis} label="Analysis model" required />
          )}
        </View>
      ) : (
        <Hint>Hidden from Ask, indexing, and failover until re-enabled.</Hint>
      )}
    </Panel>
  );
}
export function ProviderCards({ settings, form }: { settings: AISettings; form: SettingsFormModel }) {
  return (
    <View style={styles.stack}>
      {providers.map((provider) => (
        <ProviderCard key={provider.id} provider={provider} settings={settings} form={form} />
      ))}
    </View>
  );
}
export function ProviderOrder({ form, lane }: { form: SettingsFormModel; lane: 'chat' | 'analysis' }) {
  const prefix = lane === 'chat' ? 'chat_provider' : 'ai_provider';
  return (
    <Group title={lane === 'chat' ? 'Ask fallback order' : 'Document analysis failover order'}>
      <Hint>
        {lane === 'chat'
          ? 'The first backend with a configured key and an available model serves the whole turn.'
          : 'Enabled backends claim indexing and page analysis in parallel, then fail over when one is rate-limited.'}
      </Hint>
      {[1, 2, 3, 4].map((rank) => {
        const id = `${prefix}_${rank}`;
        const options = providerOptions(form.values, lane, form.values[id]);
        if (rank > 1) options.unshift({ value: '', label: 'Unused', disabled: false });
        return (
          <FormSelect
            key={id}
            form={form}
            id={id}
            label={['Primary', 'Secondary', 'Third', 'Fourth'][rank - 1]!}
            options={options}
          />
        );
      })}
      <Text style={styles.muted}>Disabled providers are not available for new selections.</Text>
    </Group>
  );
}
