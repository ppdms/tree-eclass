import { useState } from 'react';
import { aiDraft, aiNativeFields, defaults, providers, providerOptions, validateAI } from './aiModel';
import { ProviderCards, ProviderOrder } from './aiProviders';
import { FormCheck, FormField, FormSelect, SaveChanges, serializeCheckboxes, useSettingsForm } from './form';
import type { SettingsFormModel } from './form';
import type { AISettings } from '@/features/settings/types';
import { Section } from './section';
import { Group, Hint, type SettingsPayload } from './shared';

function AskModels({ form }: { form: SettingsFormModel }) {
  return (
    <Group title="Ask models">
      <Hint>Entries served only by disabled providers stay here and return to the picker when re-enabled.</Hint>
      <FormField form={form} id="chat_default_model" label="Default model" required />
      <FormField
        form={form}
        id="chat_models"
        label="Model picker choices"
        required
        hint="Comma-separated; the default must appear here."
      />
      <FormCheck
        form={form}
        id="chat_think"
        label="Ask uses the model’s reasoning tokens when the backend supports them"
      />
    </Group>
  );
}
function IndexingModels({ form }: { form: SettingsFormModel }) {
  return (
    <Group title="Document indexing">
      <FormCheck
        form={form}
        id="ai_enrichment_enabled"
        label="Index documents with AI analysis and classify new external files"
      />
      <FormSelect
        form={form}
        id="ai_provider"
        label="Analysis backend"
        options={providerOptions(form.values, 'analysis', form.values.ai_provider)}
      />
      <FormField form={form} id="ai_model" label="Primary model" required />
      <FormField
        form={form}
        id="ai_document_fallback_models"
        label="Document fallbacks"
        hint="Optional, comma-separated multimodal models."
      />
      <FormField
        form={form}
        id="ai_page_fallback_models"
        label="Page fallbacks"
        hint="Optional, comma-separated vision models."
      />
    </Group>
  );
}
function StudyModels({ form }: { form: SettingsFormModel }) {
  return (
    <Group title="Generated study material">
      <FormCheck
        form={form}
        id="ai_course_synthesis_enabled"
        label="Generate a course blueprint from indexed materials"
      />
      <FormCheck
        form={form}
        id="ai_practice_questions_enabled"
        label="Generate practice questions from course materials"
      />
      <FormField form={form} id="ai_course_model" label="Course model" required />
      <FormField
        form={form}
        id="ai_course_fallback_models"
        label="Course fallbacks"
        hint="Optional, comma-separated."
      />
      <FormField form={form} id="ai_practice_model" label="Practice model" required />
      <FormField
        form={form}
        id="ai_practice_fallback_models"
        label="Practice fallbacks"
        hint="Optional, comma-separated."
      />
    </Group>
  );
}
function AIForm({ settings }: { settings: AISettings }) {
  const [initial] = useState(() => aiDraft(settings));
  const form = useSettingsForm({
    section: 'ai',
    endpoint: '/api/v1/settings/ai',
    initial,
    validate: validateAI,
    serialize: serializeCheckboxes,
    nativeFields: aiNativeFields,
  });
  return (
    <>
      <ProviderCards settings={settings} form={form} />
      <ProviderOrder form={form} lane="chat" />
      <ProviderOrder form={form} lane="analysis" />
      <AskModels form={form} />
      <IndexingModels form={form} />
      <StudyModels form={form} />
      <SaveChanges form={form} />
    </>
  );
}
export function AISection({ data }: { data: SettingsPayload }) {
  const settings = data.ai_settings ?? defaults;
  const enabled = providers.filter((provider) => !settings.disabled_providers.includes(provider.id));
  const primary = providers.find((provider) => provider.id === settings.chat_provider_order[0]);
  return (
    <Section
      id="ai"
      title="AI backends and models"
      advanced
      status={`${enabled.length}/${providers.length} providers · ${primary?.label ?? 'none'} first for Ask`}
      description={
        'Toggle providers, route Ask, and choose indexing/study models. Keys remain in native runtime settings.'
      }
    >
      <AIForm settings={settings} />
    </Section>
  );
}
