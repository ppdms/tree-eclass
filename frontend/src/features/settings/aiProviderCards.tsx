import * as stylex from '@stylexjs/stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import type { AISettings } from './types';
import { Field } from './fields';

const styles = stylex.create({
  grid: {
    gap: '0.75rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      '@media (max-width: 720px)': 'minmax(0, 1fr)',
    },
  },
  card: {
    padding: '0.75rem',
    borderColor: colors.border,
    borderRadius: '0.75rem',
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.5rem',
    display: 'flex',
    flexDirection: 'column',
  },
  cardDisabled: {
    backgroundColor: colors.surfaceRaised,
    opacity: 0.75,
  },
  cardHead: {
    gap: '0.5rem',
    alignItems: 'center',
    display: 'flex',
  },
  cardMeta: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  cardNote: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  cardFields: {
    gap: '0.5rem',
    display: 'flex',
    flexDirection: 'column',
  },
});

export interface ProviderMeta {
  id: string;
  label: string;
  chat: boolean;
  enrich: boolean;
  chatField?: ModelFieldKey;
  enrichField?: ModelFieldKey;
  chatLabel?: string;
  enrichLabel?: string;
}

export const PROVIDERS: readonly ProviderMeta[] = [
  {
    id: 'synthetic',
    label: 'Synthetic',
    chat: true,
    enrich: true,
    chatField: 'synthetic_chat_model',
    enrichField: 'synthetic_enrichment_model',
    chatLabel: 'Ask model',
    enrichLabel: 'Analysis model',
  },
  {
    id: 'zai',
    label: 'Z.AI (GLM Coding Plan)',
    chat: false,
    enrich: true,
    enrichField: 'zai_enrichment_model',
    enrichLabel: 'Analysis model',
  },
  {
    id: 'ollama',
    label: 'Ollama Cloud',
    chat: true,
    enrich: true,
    chatField: 'ollama_chat_model',
    enrichField: 'ollama_enrichment_model',
    chatLabel: 'Ask model',
    enrichLabel: 'Analysis model',
  },
  {
    id: 'alibaba',
    label: 'Alibaba Cloud',
    chat: true,
    enrich: true,
    chatField: 'alibaba_chat_model',
    enrichField: 'alibaba_enrichment_model',
    chatLabel: 'Ask model',
    enrichLabel: 'Analysis model',
  },
  {
    id: 'huggingface',
    label: 'Hugging Face',
    chat: true,
    enrich: true,
    chatField: 'huggingface_model',
    enrichLabel: 'Analysis model',
    enrichField: 'huggingface_model',
    chatLabel: 'Model (Ask + analysis)',
  },
  {
    id: 'opencode-go',
    label: 'OpenCode Go',
    chat: true,
    enrich: false,
    chatField: 'opencode_go_model',
    chatLabel: 'Ask model',
  },
];

const STATUS_LABELS = {
  available: 'quota tracked · available',
  blocked: 'quota paused',
  limited: 'quota paused',
  unchecked: 'quota not checked yet',
  disabled: 'disabled',
  ai_disabled: 'AI analysis off',
} satisfies Record<string, string>;

export function statusLabel(status: string | undefined): string {
  if (!status || status === 'unknown') return 'worker has not reported';
  const match = Object.entries(STATUS_LABELS).find(([key]) => key === status);
  return match ? match[1] : status;
}

// Model-field getters keyed by the AISettings property each card reads.
type ModelFieldKey =
  | 'synthetic_chat_model'
  | 'synthetic_enrichment_model'
  | 'ollama_chat_model'
  | 'ollama_enrichment_model'
  | 'huggingface_model'
  | 'alibaba_chat_model'
  | 'alibaba_enrichment_model'
  | 'zai_enrichment_model'
  | 'opencode_go_model';

type ModelFieldGetter = (settings: AISettings) => string;

const MODEL_FIELD_VALUES = {
  synthetic_chat_model: (settings) => settings.synthetic_chat_model,
  synthetic_enrichment_model: (settings) => settings.synthetic_enrichment_model,
  ollama_chat_model: (settings) => settings.ollama_chat_model,
  ollama_enrichment_model: (settings) => settings.ollama_enrichment_model,
  huggingface_model: (settings) => settings.huggingface_model,
  alibaba_chat_model: (settings) => settings.alibaba_chat_model,
  alibaba_enrichment_model: (settings) => settings.alibaba_enrichment_model,
  zai_enrichment_model: (settings) => settings.zai_enrichment_model,
  opencode_go_model: (settings) => settings.opencode_go_model,
} satisfies Record<ModelFieldKey, ModelFieldGetter>;

function fieldValue(settings: AISettings, field: ModelFieldKey | undefined): string {
  return (field && MODEL_FIELD_VALUES[field]?.(settings)) ?? '';
}

function ProviderCard({ provider, settings }: { provider: ProviderMeta; settings: AISettings }) {
  const disabled = settings.disabled_providers.includes(provider.id);
  const status = settings.provider_status?.[provider.id];
  return (
    <div {...stylex.props(styles.card, disabled && styles.cardDisabled)}>
      <label {...stylex.props(styles.cardHead)}>
        <input type="checkbox" name={`provider_enabled_${provider.id}`} defaultChecked={!disabled} />
        <strong>{provider.label}</strong>
      </label>
      <p {...stylex.props(styles.cardMeta)}>
        {settings.provider_credentials?.[provider.id] ? 'Key configured' : 'No key'}
        {` · ${statusLabel(status?.status)}`}
      </p>
      {disabled ? (
        <>
          <p {...stylex.props(styles.cardNote)}>Hidden from Ask, indexing, and failover until re-enabled.</p>
          {provider.chat && (
            <input type="hidden" name={provider.chatField} value={fieldValue(settings, provider.chatField)} />
          )}
          {provider.enrich && provider.enrichField !== provider.chatField && (
            <input type="hidden" name={provider.enrichField} value={fieldValue(settings, provider.enrichField)} />
          )}
        </>
      ) : (
        <div {...stylex.props(styles.cardFields)}>
          {provider.chat && (
            <Field
              label={provider.chatLabel ?? 'Ask model'}
              name={provider.chatField ?? ''}
              defaultValue={fieldValue(settings, provider.chatField)}
              required
            />
          )}
          {provider.enrich && provider.enrichField !== provider.chatField && (
            <Field
              label={provider.enrichLabel ?? 'Analysis model'}
              name={provider.enrichField ?? ''}
              defaultValue={fieldValue(settings, provider.enrichField)}
              required
            />
          )}
        </div>
      )}
    </div>
  );
}

export function ProviderCards({ settings }: { settings: AISettings }) {
  return (
    <div {...stylex.props(styles.grid)}>
      {PROVIDERS.map((provider) => (
        <ProviderCard key={provider.id} provider={provider} settings={settings} />
      ))}
    </div>
  );
}
