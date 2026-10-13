import type {
  KnowledgeGuideDiagnostic,
  KnowledgeRoadmapDiagnostic,
  KnowledgeSourceDiagnostic,
  KnowledgeStatusPayload,
  KnowledgeStatusRow,
} from '@/features/settings/types';
import type { SettingsCourse } from './shared';
import { lookup } from '@/lib/display';

export function knowledgeTotals(status: KnowledgeStatusPayload | null) {
  const totals = { supported: 0, indexed: 0, failed: 0, pending: 0 };
  for (const row of status?.coverage ?? []) {
    totals.supported += Number(row.supported_documents || 0);
    totals.indexed += Number(row.indexed_documents || 0);
    totals.failed += Number(row.failed_documents || 0);
    totals.pending += Number(row.pending_documents || 0);
  }
  return totals;
}
export function coverageState(row?: KnowledgeStatusRow) {
  const supported = Number(row?.supported_documents || 0),
    indexed = Number(row?.indexed_documents || 0);
  const failed = Number(row?.failed_documents || 0),
    pending = Number(row?.pending_documents || 0);
  const percent = supported ? Math.round((indexed / supported) * 100) : 0;
  return {
    supported,
    indexed,
    failed,
    pending,
    percent,
    state: failed ? 'failed' : pending || percent < 100 ? 'pending' : 'ready',
  };
}
export function courseLabel(courses: SettingsCourse[], id?: number | string) {
  const course = courses.find((item) => String(item.id) === String(id));
  return course?.short_name || course?.name || (id ? `Course ${id}` : 'Unknown course');
}
export const guideStatusLabels = {
  not_queued: 'not queued',
  pending: 'queued',
  running: 'processing',
  stale: 'outdated',
} satisfies Record<string, string>;
export function guideExplanation(row: KnowledgeGuideDiagnostic) {
  const reason = row.reason || row.status || 'unknown';
  if (reason === 'generation_failed') return row.error || 'The last guide generation failed.';
  if (reason === 'stale_generation') {
    return `This guide was generated with ${row.model || 'an older model'} and is not current.`;
  }
  if (reason === 'processing') return 'A worker is generating the guide now.';
  if (reason === 'queued') return 'The guide is queued and waiting for an available worker.';
  if (reason === 'waiting_for_page_analysis') {
    const ready = Number(row.page_ready || 0);
    const total = Number(row.page_total || 0) || '?';
    return `The visual page scan must finish first (${ready}/${total} pages ready).`;
  }
  if (reason === 'no_extractable_content') return 'There is no extractable text or visual page content for a guide.';
  return 'No current guide generation exists for this indexed file.';
}
const roadmapReasons = {
  documents_pending_extraction: 'source files are still being extracted',
  extraction_jobs_active: 'source extraction jobs are active',
  document_enrichments_active: 'file guides are being generated',
  page_enrichments_active: 'visual page scans are active',
  document_enrichments_missing_current_generation: 'some files have no guide for the current model/version',
  documents_unverified_content: 'some source bytes still need verification',
  no_ready_documents: 'there are no ready source files yet',
  no_ready_document_insights: 'no file guide is ready yet',
} satisfies Record<string, string>;
export function roadmapExplanation(row: KnowledgeRoadmapDiagnostic) {
  if (row.error && row.status === 'failed') return row.error;
  const reasons = row.readiness?.blocking_reasons ?? [];
  if (reasons.length) {
    return reasons.map((reason) => lookup(roadmapReasons, reason, reason.replaceAll('_', ' '))).join('; ');
  }
  if (row.status === 'pending' || row.status === 'running') return 'The roadmap generation job is queued or running.';
  if (row.status === 'missing') return 'No roadmap generation has been created yet.';
  if (row.status === 'failed')
    return 'The last roadmap generation failed; retry failed work from Knowledge maintenance.';
  return 'The roadmap is ready or waiting for a refresh.';
}
export function roadmapStats(readiness: KnowledgeRoadmapDiagnostic['readiness']) {
  if (!readiness) return '';
  const docs = Number(readiness.ready_documents || 0),
    guides = Number(readiness.ready_document_insights || 0);
  const active =
    Number(readiness.active_extraction_jobs || 0) +
    Number(readiness.active_document_enrichments || 0) +
    Number(readiness.active_page_enrichments || 0);
  return `${guides}/${docs} file guides ready${active ? ` · ${active} upstream job(s) active` : ''}`;
}
export function sourceExplanation(row: KnowledgeSourceDiagnostic) {
  if (row.diagnostic_reason === 'unsupported_mime_type') return 'This file type is not supported by the extractor.';
  if (row.diagnostic_reason === 'skipped_limit') return 'The indexing safety limit skipped this file.';
  return row.error || row.diagnostic_reason?.replaceAll('_', ' ') || 'The source could not be indexed.';
}
