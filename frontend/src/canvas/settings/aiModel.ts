import type { AISettings } from '@/features/settings/types';
import type { FormValues, Validation } from './form';

export interface Provider {
  id: string;
  label: string;
  chat?: string;
  analysis?: string;
}
export const providers: readonly Provider[] = [
  { id: 'synthetic', label: 'Synthetic', chat: 'synthetic_chat_model', analysis: 'synthetic_enrichment_model' },
  { id: 'zai', label: 'Z.AI (GLM Coding Plan)', analysis: 'zai_enrichment_model' },
  { id: 'ollama', label: 'Ollama Cloud', chat: 'ollama_chat_model', analysis: 'ollama_enrichment_model' },
  { id: 'alibaba', label: 'Alibaba Cloud', chat: 'alibaba_chat_model', analysis: 'alibaba_enrichment_model' },
  { id: 'huggingface', label: 'Hugging Face', chat: 'huggingface_model', analysis: 'huggingface_model' },
  { id: 'opencode-go', label: 'OpenCode Go', chat: 'opencode_go_model' },
];
export const defaults: AISettings = {
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
export const scalarModels = [
  'chat_default_model',
  'synthetic_chat_model',
  'ollama_chat_model',
  'alibaba_chat_model',
  'opencode_go_model',
  'ai_model',
  'synthetic_enrichment_model',
  'ollama_enrichment_model',
  'huggingface_model',
  'alibaba_enrichment_model',
  'zai_enrichment_model',
  'ai_course_model',
  'ai_practice_model',
] as const;
const modelLists = [
  'chat_models',
  'ai_document_fallback_models',
  'ai_page_fallback_models',
  'ai_course_fallback_models',
  'ai_practice_fallback_models',
] as const;
const booleans = [
  'chat_think',
  'ai_enrichment_enabled',
  'ai_course_synthesis_enabled',
  'ai_practice_questions_enabled',
] as const;

export function aiDraft(settings: AISettings): FormValues {
  const result: FormValues = { ai_provider: settings.ai_provider };
  for (const key of scalarModels) result[key] = settings[key];
  for (const key of modelLists) result[key] = settings[key].join(', ');
  for (const key of booleans) result[key] = settings[key] ? 'on' : 'false';
  for (const provider of providers) {
    result[`provider_enabled_${provider.id}`] = settings.disabled_providers.includes(provider.id) ? 'false' : 'on';
  }
  for (let rank = 1; rank <= 4; rank++) {
    result[`chat_provider_${rank}`] = settings.chat_provider_order[rank - 1] ?? '';
    result[`ai_provider_${rank}`] = settings.ai_provider_order[rank - 1] ?? '';
  }
  return result;
}

function validateLane(values: FormValues, lane: 'chat' | 'analysis'): Validation {
  const allowed = providers.filter((provider) => provider[lane]);
  const prefix = lane === 'chat' ? 'chat_provider' : 'ai_provider';
  const order = [1, 2, 3, 4].map((rank) => values[`${prefix}_${rank}`]?.trim()).filter(Boolean);
  if (!order.length) return { field: `${prefix}_1`, message: 'At least one backend must be enabled.' };
  if (new Set(order).size !== order.length) {
    return { field: `${prefix}_1`, message: 'Each backend may appear only once.' };
  }
  if (order.some((id) => !allowed.some((provider) => provider.id === id))) {
    return { field: `${prefix}_1`, message: 'Unsupported backend in the fallback order.' };
  }
  if (lane === 'chat' && !order.some((id) => values[`provider_enabled_${id}`] === 'on')) {
    return { field: `${prefix}_1`, message: 'At least one Ask backend must remain enabled.' };
  }
  return null;
}

function validateAIProviders(values: FormValues): Validation {
  const enabled = providers.filter((provider) => values[`provider_enabled_${provider.id}`] === 'on');
  if (!enabled.length) return { message: 'At least one provider must remain enabled.' };
  if (!enabled.some((provider) => provider.analysis)) {
    return { message: 'At least one document analysis backend must remain enabled.' };
  }
  const laneError = validateLane(values, 'chat') ?? validateLane(values, 'analysis');
  if (laneError) return laneError;
  if (!providers.some((provider) => provider.analysis && provider.id === values.ai_provider)) {
    return { field: 'ai_provider', message: 'Unsupported document analysis backend.' };
  }
  return null;
}
function validateAIModels(values: FormValues): Validation {
  for (const key of scalarModels) {
    const value = values[key]?.trim() ?? '';
    if (!value || Array.from(value).length > 200 || /\s/u.test(value)) {
      return { field: key, message: 'Model names must contain 1 to 200 characters without spaces.' };
    }
  }
  for (const key of modelLists) {
    const list = [
      ...new Set(
        (values[key] ?? '')
          .split(',')
          .map((value) => value.trim())
          .filter(Boolean),
      ),
    ];
    if (list.length > 30) return { field: key, message: 'A model list may contain at most 30 models.' };
    if (list.some((value) => Array.from(value).length > 200 || /\s/u.test(value))) {
      return { field: key, message: 'Model names must contain 1 to 200 characters without spaces.' };
    }
  }
  if (
    !(values.chat_models ?? '')
      .split(',')
      .map((value) => value.trim())
      .includes((values.chat_default_model ?? '').trim())
  ) {
    return { field: 'chat_models', message: 'Default Ask model must also appear in the Ask model list.' };
  }
  return null;
}

export function validateAI(values: FormValues): Validation {
  return validateAIProviders(values) ?? validateAIModels(values);
}

export function providerOptions(values: FormValues, lane: 'chat' | 'analysis', selected = '') {
  return providers
    .filter(
      (provider) => provider[lane] && (values[`provider_enabled_${provider.id}`] === 'on' || selected === provider.id),
    )
    .map((provider) => ({
      value: provider.id,
      label: provider.label,
      disabled: values[`provider_enabled_${provider.id}`] !== 'on',
    }));
}

export function aiNativeFields(values: FormValues): string[] {
  const fields = [
    'chat_default_model',
    'chat_models',
    'ai_model',
    'ai_document_fallback_models',
    'ai_page_fallback_models',
    'ai_course_model',
    'ai_course_fallback_models',
    'ai_practice_model',
    'ai_practice_fallback_models',
  ];
  for (const provider of providers) {
    if (values[`provider_enabled_${provider.id}`] !== 'on') continue;
    if (provider.chat) fields.push(provider.chat);
    if (provider.analysis && provider.analysis !== provider.chat) fields.push(provider.analysis);
  }
  return fields;
}
