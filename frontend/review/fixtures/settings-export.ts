import type { JsonValue } from '@/lib/display';

// Synthetic envelope authored from the Go export allowlist, never from live learner data.
export const learnerExport = {
  format: 'tree-eClass learner data export',
  version: 2,
  schema_engine: 'goose',
  schema_version: 1,
  exported_at: '2026-10-13T10:00:00Z',
  courses: [{ id: 101, name: 'Linear Algebra', code: 'MATH101' }],
  settings: {
    preferences: { check_interval_minutes: 60, download_base_path: '/SyntheticUniversity' },
    planner: { weekly_minutes: { '101': 120 }, block_minutes: 30 },
    course_exam_plans: [],
    discord_course_channels: [],
  },
  study: {
    study_sessions: [],
    study_unit_events: [],
    study_plan_items: [],
    study_review_overrides: [],
    file_study: [],
    collapsed_course_folders: [],
    practice_attempts: [],
    study_workspace_sessions: [],
    study_reading_spans: [],
    study_reading_beats: [],
    annotations: [],
  },
  conversations: [],
  privacy: {
    excluded: [
      'credentials',
      'webhook_config',
      'webdav_config',
      'app_data',
      'discord_export_settings.token',
      'provider_keys',
    ],
    note: 'Secrets and session cookies are never included in exports.',
  },
} satisfies JsonValue;
