import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import type { AISettings, SettingsPayload } from './types';
import { colors, typography } from '@/styles/tokens.stylex';
import { Field } from './fields';
import { Section, SettingsForm } from './settingsForm';
import { PROVIDERS, ProviderCards, type ProviderMeta } from './aiProviderCards';

const styles = stylex.create({
  formGroup: {
    gap: '0.35rem',
    display: 'flex',
    flexDirection: 'column',
    marginBottom: '1rem',
  },
  select: {
    borderColor: {
      default: colors.border,
      ':hover:not(:disabled)': colors.borderLight,
      ':focus': colors.focusRing,
    },
    borderRadius: '0.5rem',
    borderStyle: 'solid',
    borderWidth: 1,
    outline: 'none',
    paddingInline: '0.75rem',
    backgroundColor: colors.surface,
    boxShadow: {
      default: 'none',
      ':focus': `0 0 0 0.1875rem color-mix(in srgb, ${colors.focusRing} 28%, transparent)`,
    },
    boxSizing: 'border-box',
    color: colors.textPrimary,
    fontFamily: 'inherit',
    fontSize: typography.sizeBase,
    minHeight: '2.5rem',
    minWidth: 0,
    width: '100%',
  },
  settingsSectionDescription: {
    marginBlock: '0.35rem',
    marginInline: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    marginBottom: '0.75rem',
    width: 'auto',
  },
  settingsMatrix: {
    gap: '1rem',
    display: 'grid',
    gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  },
  checkRow: {
    gap: '0.5rem',
    alignItems: 'center',
    display: 'flex',
    minHeight: '2.5rem',
  },
});

const RANK_LABELS = ['Primary', 'Secondary', 'Third', 'Fourth'];

const DEFAULTS: AISettings = {
  disabled_providers: ['ollama', 'alibaba'],
  chat_provider_order: ['alibaba', 'synthetic', 'ollama', 'opencode-go'],
  chat_default_model: 'qwen3.8-flash',
  chat_models: ['syn:large:text', 'syn:small:text', 'glm-5.2', 'qwen3.8-flash'],
  chat_think: true,
  synthetic_chat_model: 'syn:large:text',
  ollama_chat_model: 'glm-5.2',
  alibaba_chat_model: 'qwen3.8-flash',
  opencode_go_model: 'glm-5.2',
  ai_provider: 'zai',
  ai_enrichment_enabled: true,
  ai_model: 'glm-5.3-flash',
  ai_provider_order: ['zai', 'synthetic', 'ollama'],
  synthetic_enrichment_model: 'hf:zai-org/GLM-5.3-Flash',
  ollama_enrichment_model: 'minimax-m3',
  huggingface_model: 'zai-org/GLM-5.3-Flash',
  alibaba_enrichment_model: 'qwen3.8-flash',
  zai_enrichment_model: 'glm-5.3-flash',
  ai_document_fallback_models: [],
  ai_page_fallback_models: [],
  ai_course_synthesis_enabled: true,
  ai_course_model: 'glm-5.3',
  ai_course_fallback_models: ['glm-5.3-flash'],
  ai_practice_questions_enabled: true,
  ai_practice_model: 'glm-5.3',
  ai_practice_fallback_models: ['glm-5.3-flash'],
  provider_credentials: {},
  provider_status: {},
};

function isDisabled(settings: AISettings, provider: ProviderMeta): boolean {
  return settings.disabled_providers.includes(provider.id);
}

function RankSelect({
  rank,
  value,
  name,
  options,
}: {
  rank: number;
  value: string | undefined;
  name: string;
  options: readonly ProviderMeta[];
}) {
  return (
    <div {...stylex.props(styles.formGroup)}>
      <label htmlFor={`${name}_${rank}`}>{RANK_LABELS[rank - 1]}</label>
      <select id={`${name}_${rank}`} name={`${name}_${rank}`} defaultValue={value} {...stylex.props(styles.select)}>
        {rank > 1 && <option value="">Unused</option>}
        {options.map((provider) => (
          <option value={provider.id} key={provider.id}>
            {provider.label}
          </option>
        ))}
      </select>
    </div>
  );
}

function AskProviderOrder({ settings }: { settings: AISettings }) {
  const enabled = PROVIDERS.filter((provider) => provider.chat && !isDisabled(settings, provider));
  return (
    <fieldset>
      <legend>Ask fallback order</legend>
      <p {...stylex.props(styles.settingsSectionDescription)}>
        The first backend with a configured key and an available model serves the whole turn. Disabled providers are not
        listed.
      </p>
      <div {...stylex.props(styles.settingsMatrix)}>
        {[1, 2, 3, 4].map((rank) => (
          <RankSelect
            rank={rank}
            value={settings.chat_provider_order[rank - 1]}
            name="chat_provider"
            options={enabled}
            key={rank}
          />
        ))}
      </div>
    </fieldset>
  );
}

function AnalysisProviderOrder({ settings }: { settings: AISettings }) {
  const enabled = PROVIDERS.filter((provider) => provider.enrich && !isDisabled(settings, provider));
  return (
    <fieldset>
      <legend>Document analysis failover order</legend>
      <p {...stylex.props(styles.settingsSectionDescription)}>
        Indexing and page analysis claim work from every enabled backend in parallel and fail over to the next when one
        is rate-limited.
      </p>
      <div {...stylex.props(styles.settingsMatrix)}>
        {[1, 2, 3, 4].map((rank) => (
          <RankSelect
            rank={rank}
            value={settings.ai_provider_order[rank - 1]}
            name="ai_provider"
            options={enabled}
            key={rank}
          />
        ))}
      </div>
    </fieldset>
  );
}

function AskModels({ settings }: { settings: AISettings }) {
  return (
    <fieldset>
      <legend>Ask models</legend>
      <p {...stylex.props(styles.settingsSectionDescription)}>
        Models served by disabled providers are hidden from the picker at runtime; their entries are kept here so
        re-enabling restores them.
      </p>
      <div {...stylex.props(styles.settingsMatrix)}>
        <Field label="Default model" name="chat_default_model" defaultValue={settings.chat_default_model} required />
        <Field
          label="Model picker choices"
          name="chat_models"
          defaultValue={settings.chat_models.join(', ')}
          hint="Comma-separated; the default must appear here."
          required
        />
      </div>
      <label {...stylex.props(styles.checkRow)}>
        <input type="checkbox" name="chat_think" defaultChecked={settings.chat_think} />
        Ask uses the model’s reasoning tokens when the backend supports them
      </label>
    </fieldset>
  );
}

function EnrichmentModels({ settings }: { settings: AISettings }) {
  const enabled = PROVIDERS.filter((provider) => provider.enrich && !isDisabled(settings, provider));
  return (
    <fieldset>
      <legend>Document indexing</legend>
      <label {...stylex.props(styles.checkRow)}>
        <input type="checkbox" name="ai_enrichment_enabled" defaultChecked={settings.ai_enrichment_enabled} />
        Index documents with AI analysis and classify new external files
      </label>
      <div {...stylex.props(styles.settingsMatrix)}>
        <div {...stylex.props(styles.formGroup)}>
          <label htmlFor="ai_provider">Analysis backend</label>
          <select
            id="ai_provider"
            name="ai_provider"
            defaultValue={settings.ai_provider}
            {...stylex.props(styles.select)}
          >
            {enabled.map((provider) => (
              <option value={provider.id} key={provider.id}>
                {provider.label}
              </option>
            ))}
          </select>
        </div>
        <Field label="Primary model" name="ai_model" defaultValue={settings.ai_model} required />
        <Field
          label="Document fallbacks"
          name="ai_document_fallback_models"
          defaultValue={settings.ai_document_fallback_models.join(', ')}
          hint="Optional, comma-separated multimodal models."
        />
        <Field
          label="Page fallbacks"
          name="ai_page_fallback_models"
          defaultValue={settings.ai_page_fallback_models.join(', ')}
          hint="Optional, comma-separated vision models."
        />
      </div>
    </fieldset>
  );
}

function GeneratedStudyModels({ settings }: { settings: AISettings }) {
  return (
    <fieldset>
      <legend>Generated study material</legend>
      <div {...stylex.props(styles.settingsMatrix)}>
        <label {...stylex.props(styles.checkRow)}>
          <input
            type="checkbox"
            name="ai_course_synthesis_enabled"
            defaultChecked={settings.ai_course_synthesis_enabled}
          />
          Generate a course blueprint from indexed materials
        </label>
        <label {...stylex.props(styles.checkRow)}>
          <input
            type="checkbox"
            name="ai_practice_questions_enabled"
            defaultChecked={settings.ai_practice_questions_enabled}
          />
          Generate practice questions from course materials
        </label>
        <Field label="Course model" name="ai_course_model" defaultValue={settings.ai_course_model} required />
        <Field
          label="Course fallbacks"
          name="ai_course_fallback_models"
          defaultValue={settings.ai_course_fallback_models.join(', ')}
        />
        <Field label="Practice model" name="ai_practice_model" defaultValue={settings.ai_practice_model} required />
        <Field
          label="Practice fallbacks"
          name="ai_practice_fallback_models"
          defaultValue={settings.ai_practice_fallback_models.join(', ')}
        />
      </div>
    </fieldset>
  );
}

export function AIBackendSettings({ data, dirtySections }: { data: SettingsPayload; dirtySections: Set<string> }) {
  const settings = data.ai_settings || DEFAULTS;
  const enabled = PROVIDERS.filter((provider) => !settings.disabled_providers.includes(provider.id));
  const chatPrimary = PROVIDERS.find((provider) => provider.id === settings.chat_provider_order[0]);
  return (
    <Section
      id="ai"
      title="AI backends and models"
      collapsible
      defaultOpen={false}
      status={`${enabled.length}/${PROVIDERS.length} providers · ${chatPrimary?.label ?? 'none'} first for Ask`}
      dirty={dirtySections.has('ai')}
      description={
        'Toggle providers, then route Ask requests and choose models for indexing and generated study material. ' +
        'Disabled providers are hidden from every lane until re-enabled. API keys remain in systemd credentials.'
      }
    >
      <ProviderCards settings={settings} />
      <SettingsForm action="/api/settings/ai">
        <AskProviderOrder settings={settings} />
        <AnalysisProviderOrder settings={settings} />
        <AskModels settings={settings} />
        <EnrichmentModels settings={settings} />
        <GeneratedStudyModels settings={settings} />
      </SettingsForm>
    </Section>
  );
}
