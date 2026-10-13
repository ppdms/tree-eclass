import { http, HttpResponse, passthrough, type HttpHandler } from 'msw';
import { z } from 'zod/v4';
import type { CourseSummary } from '@/lib/types';
import type { AvailableCourse } from '@/features/settings/addCourseCatalog';
import type {
  AISettings,
  CheckStatusPayload,
  KnowledgeStatusPayload,
  SettingsPayload,
  SyncStatusPayload,
} from '@/features/settings/types';
import type { ReviewFixture } from '../scenario';

const preferencesSchema = z.object({
  check_interval_minutes: z.string(),
  download_base_path: z.string(),
  global_feed_dept_enabled: z.string().optional(),
  global_feed_undergrad_enabled: z.string().optional(),
  global_feed_rector_enabled: z.string().optional(),
});
const aiFormSchema = z.object({
  chat_default_model: z.string(),
  chat_models: z.string(),
  chat_think: z.string().optional(),
});

const algebra: CourseSummary = { id: 101, name: 'Linear Algebra', code: 'MATH101', total_files: 4, indexed_files: 4 };
const algorithms: CourseSummary = { id: 202, name: 'Algorithms', code: 'CS202', total_files: 3, indexed_files: 3 };
const statistics: CourseSummary = { id: 303, name: 'Statistics', code: 'STAT303', hidden: true };
const aiSettings: AISettings = {
  disabled_providers: ['synthetic', 'alibaba', 'opencode-go', 'huggingface', 'zai'],
  chat_provider_order: ['ollama'],
  chat_default_model: 'review-local-model',
  chat_models: ['review-local-model', 'review-small-model'],
  chat_think: false,
  synthetic_chat_model: 'review-synthetic-model',
  ollama_chat_model: 'review-local-model',
  alibaba_chat_model: 'review-alibaba-model',
  opencode_go_model: 'review-opencode-model',
  ai_provider: 'ollama',
  ai_enrichment_enabled: true,
  ai_model: 'review-local-model',
  ai_provider_order: ['ollama'],
  synthetic_enrichment_model: 'review-synthetic-analysis',
  ollama_enrichment_model: 'review-local-model',
  huggingface_model: 'review-huggingface-model',
  alibaba_enrichment_model: 'review-alibaba-model',
  zai_enrichment_model: 'review-zai-model',
  ai_document_fallback_models: [],
  ai_page_fallback_models: [],
  ai_course_synthesis_enabled: true,
  ai_course_model: 'review-local-model',
  ai_course_fallback_models: [],
  ai_practice_questions_enabled: true,
  ai_practice_model: 'review-local-model',
  ai_practice_fallback_models: [],
  provider_credentials: {},
  provider_status: { ollama: { key: false, status: 'available' } },
};

function settingsPayload(): SettingsPayload {
  return {
    courses: [{ ...algebra }],
    hidden_courses: [{ ...statistics }],
    preferences: {
      check_interval_minutes: 60,
      download_base_path: '/SyntheticUniversity',
      global_feed_dept_enabled: true,
      global_feed_undergrad_enabled: false,
      global_feed_rector_enabled: false,
    },
    has_credentials: false,
    storage: { configured: true },
    discord_channels: [],
    discord_mapped_count: 0,
    discord_exporter: {
      enabled: false,
      media: false,
      interval_seconds: 600,
      parallel: 1,
      include_threads: 'none',
    },
    discord_export_token_configured: false,
    ai_settings: structuredClone(aiSettings),
  };
}

type SettingsState =
  | 'normal'
  | 'save-error'
  | 'provider-validation'
  | 'connection-unavailable'
  | 'catalog-loading'
  | 'load-error'
  | 'course-error';
interface FixtureState {
  data: SettingsPayload;
  reads: number;
  preferenceSaves: number;
  aiSaves: number;
  courseAdds: number;
  connectionTests: number;
}

function gate() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

function catalog(data: SettingsPayload): AvailableCourse[] {
  return [algebra, algorithms, statistics].map((course) => ({
    id: String(course.id),
    code: course.code || '',
    title: course.name,
    name: course.name,
    professor: 'Synthetic instructor',
    registered: [...(data.courses || []), ...(data.hidden_courses || [])].some((tracked) => tracked.id === course.id),
  }));
}

function readHandlers(state: SettingsState, store: FixtureState, ready: Promise<void>): HttpHandler[] {
  return [
    http.get('/api/v1/settings', () => {
      if (state === 'load-error' && ++store.reads === 1)
        return HttpResponse.json({ detail: 'Settings temporarily unavailable' }, { status: 503 });
      return HttpResponse.json(store.data);
    }),
    http.get('/api/v1/courses/available', async () => {
      await ready;
      return HttpResponse.json({ courses: catalog(store.data) });
    }),
    http.get('/api/check-status', () =>
      HttpResponse.json({
        is_checking: false,
        last_check_at: '2026-10-13T09:00:00Z',
        last_check_result: 'ok',
        last_files_added: 2,
      } satisfies CheckStatusPayload),
    ),
    http.get('/api/settings/sync-status', () =>
      HttpResponse.json({
        sync: { eclass: { last_run_at: '2026-10-13T09:00:00Z', last_result: 'ok' } },
        check: {},
      } satisfies SyncStatusPayload),
    ),
    http.get('/api/knowledge/summary', () =>
      HttpResponse.json({
        coverage: [
          {
            course_id: 101,
            discovered_documents: 4,
            indexed_documents: 4,
            supported_documents: 4,
            failed_documents: 0,
            pending_documents: 0,
          },
        ],
        ai_pipeline: { enabled: true, model: 'review-local-model', page_analysis_enabled: true },
      } satisfies KnowledgeStatusPayload),
    ),
    // Browser downloads use the generated read-only HTTP fixture, not a
    // service-worker-transferred response stream.
    http.get('/api/settings/export', () => passthrough()),
  ];
}

function saveHandlers(state: SettingsState, store: FixtureState): HttpHandler[] {
  return [
    http.post('/api/v1/settings/preferences', async ({ request }) => {
      const body = preferencesSchema.parse(await request.json());
      if (state === 'save-error' && ++store.preferenceSaves === 1)
        return HttpResponse.json({ detail: 'Preferences could not be saved. Please try again.' }, { status: 503 });
      store.data.preferences = {
        check_interval_minutes: Number(body.check_interval_minutes),
        download_base_path: body.download_base_path,
        global_feed_dept_enabled: body.global_feed_dept_enabled === 'on',
        global_feed_undergrad_enabled: body.global_feed_undergrad_enabled === 'on',
        global_feed_rector_enabled: body.global_feed_rector_enabled === 'on',
      };
      return HttpResponse.json({ ok: true });
    }),
    http.post('/api/v1/settings/ai', async ({ request }) => {
      const body = aiFormSchema.parse(await request.json());
      if (state === 'provider-validation' && ++store.aiSaves === 1) {
        return HttpResponse.json(
          { detail: 'The default model must appear in the model picker choices.' },
          { status: 422 },
        );
      }
      store.data.ai_settings = {
        ...store.data.ai_settings!,
        chat_default_model: body.chat_default_model,
        chat_models: body.chat_models.split(',').map((model) => model.trim()),
        chat_think: body.chat_think === 'on',
      };
      return HttpResponse.json({ ok: true });
    }),
    http.post('/api/settings/test-storage', () => {
      if (state === 'connection-unavailable' && ++store.connectionTests === 1)
        return HttpResponse.json({ ok: false, message: 'Local document storage is unavailable.' }, { status: 503 });
      return HttpResponse.json({ ok: true, message: 'Local document storage is ready.' });
    }),
  ];
}

function courseHandlers(state: SettingsState, store: FixtureState): HttpHandler[] {
  return [
    http.post('/api/v1/courses', async ({ request }) => {
      const body = z
        .object({
          course_id: z.number(),
          name: z.string(),
          short_name: z.string().optional(),
        })
        .parse(await request.json());
      if (state === 'course-error' && ++store.courseAdds === 1)
        return HttpResponse.json({ detail: 'The course could not be added. Try again.' }, { status: 503 });
      store.data.courses = [...(store.data.courses || []), { ...algorithms, name: body.name }];
      return HttpResponse.json({ ok: true }, { status: 201 });
    }),
    http.post('/api/v1/courses/303/show', () => {
      store.data.hidden_courses = [];
      store.data.courses = [...(store.data.courses || []), { ...statistics, hidden: false }];
      return HttpResponse.json({ ok: true });
    }),
  ];
}

export function settingsFixture(state: SettingsState = 'normal'): ReviewFixture & { release: () => void } {
  let ready = gate();
  return {
    id: `synthetic-settings-${state}-v1`,
    release: () => ready.release(),
    handlers: () => {
      ready = gate();
      if (state !== 'catalog-loading') ready.release();
      const store: FixtureState = {
        data: settingsPayload(),
        reads: 0,
        preferenceSaves: 0,
        aiSaves: 0,
        courseAdds: 0,
        connectionTests: 0,
      };
      return [
        ...readHandlers(state, store, ready.promise),
        ...saveHandlers(state, store),
        ...courseHandlers(state, store),
      ];
    },
  };
}
