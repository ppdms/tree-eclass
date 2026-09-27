import type { CourseSummary } from '@/lib/types';

// ── Settings / status payloads ─────────────────────────────────────────────

export interface SettingsDiscordChannel {
  root_id: string | number;
  name: string;
  mapped_course_id?: string | number;
}

export interface SettingsPayload {
  courses?: CourseSummary[];
  hidden_courses?: CourseSummary[];
  preferences?: {
    check_interval_minutes?: number;
    download_base_path?: string;
    global_feed_dept_enabled?: boolean;
    global_feed_undergrad_enabled?: boolean;
    global_feed_rector_enabled?: boolean;
  };
  discord_channels?: SettingsDiscordChannel[];
  discord_mapped_count?: number;
  discord_exporter?: {
    enabled?: boolean;
    media?: boolean;
    interval_seconds?: number;
    parallel?: number;
    include_threads?: string;
  };
  discord_export_token_configured?: boolean;
  has_credentials?: boolean;
  credential_username?: string;
  storage?: {
    configured: boolean;
  };
  ai_settings?: AISettings;
}

export interface AISettings {
  disabled_providers: string[];
  chat_provider_order: string[];
  chat_default_model: string;
  chat_models: string[];
  chat_think: boolean;
  synthetic_chat_model: string;
  ollama_chat_model: string;
  alibaba_chat_model: string;
  opencode_go_model: string;
  ai_provider: string;
  ai_enrichment_enabled: boolean;
  ai_model: string;
  ai_provider_order: string[];
  synthetic_enrichment_model: string;
  ollama_enrichment_model: string;
  huggingface_model: string;
  alibaba_enrichment_model: string;
  zai_enrichment_model: string;
  ai_document_fallback_models: string[];
  ai_page_fallback_models: string[];
  ai_course_synthesis_enabled: boolean;
  ai_course_model: string;
  ai_course_fallback_models: string[];
  ai_practice_questions_enabled: boolean;
  ai_practice_model: string;
  ai_practice_fallback_models: string[];
  provider_credentials: Record<string, boolean>;
  provider_status: Record<string, ProviderStatus>;
}

export interface ProviderStatus {
  key: boolean;
  status: string;
}

export interface SyncStatusEntry {
  last_run_at?: string;
  last_result?: string;
  last_error?: string;
}

export interface SyncStatusPayload {
  sync?: Record<string, SyncStatusEntry>;
  check?: { last_error?: string };
}

export interface KnowledgeStatusRow {
  course_id?: string | number;
  discovered_documents?: number;
  indexed_documents?: number;
  failed_documents?: number;
  pending_documents?: number;
  supported_documents?: number;
  unsupported_documents?: number;
  external_documents?: number;
}

export interface KnowledgeGuideSummary {
  course_id?: string | number;
  status?: string;
  count?: number;
}

export interface KnowledgeGuideDiagnostic {
  document_id?: string | number;
  course_id?: string | number;
  display_name?: string;
  source_path?: string;
  document_kind?: string;
  source_origin?: string;
  status?: string;
  reason?: string;
  model?: string;
  attempts?: number;
  available_at?: string;
  generated_at?: string;
  error?: string;
  page_total?: number;
  page_ready?: number;
  page_failed?: number;
}

export interface KnowledgeRoadmapReadiness {
  settled?: boolean;
  ready?: boolean;
  degraded?: boolean;
  blocking_reasons?: string[];
  ready_documents?: number;
  ready_document_insights?: number;
  documents_without_enrichment?: number;
  active_extraction_jobs?: number;
  active_document_enrichments?: number;
  active_page_enrichments?: number;
  failed_documents?: number;
  failed_extraction_jobs?: number;
  failed_document_enrichments?: number;
  failed_page_enrichments?: number;
}

export interface KnowledgeRoadmapDiagnostic {
  course_id?: string | number;
  status?: string;
  error?: string;
  attempts?: number;
  usable_revision?: number;
  readiness?: KnowledgeRoadmapReadiness;
}

export interface KnowledgeSourceDiagnostic {
  course_id?: string | number;
  status?: string;
  document_kind?: string;
  display_name?: string;
  source_path?: string;
  diagnostic_reason?: string;
  error?: string;
  mime_type?: string;
  response_mime_type?: string;
}

export interface KnowledgeStatusPayload {
  coverage?: KnowledgeStatusRow[];
  guide_summary?: KnowledgeGuideSummary[];
  guide_diagnostics?: KnowledgeGuideDiagnostic[];
  guide_diagnostics_truncated?: boolean;
  roadmap_diagnostics?: KnowledgeRoadmapDiagnostic[];
  ai_pipeline?: {
    enabled?: boolean;
    model?: string;
    page_analysis_enabled?: boolean;
    course_synthesis_enabled?: boolean;
    course_model?: string;
  };
  unsupported_documents?: KnowledgeSourceDiagnostic[];
  failed_documents?: KnowledgeSourceDiagnostic[];
  diagnostics_truncated?: boolean;
}

export interface CheckStatusPayload {
  is_checking?: boolean;
  course_name?: string;
  last_check_at?: string;
  last_check_result?: string;
  last_error?: string;
  last_files_added?: number;
  last_files_changed?: number;
}
