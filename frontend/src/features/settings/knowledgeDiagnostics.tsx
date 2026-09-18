import * as stylex from '@stylexjs/stylex';
import type { CourseSummary } from '@/lib/types';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import type {
  KnowledgeGuideDiagnostic,
  KnowledgeRoadmapDiagnostic,
  KnowledgeSourceDiagnostic,
  KnowledgeStatusPayload,
} from './types';

const styles = stylex.create({
  diagnostics: {
    gap: '.5rem',
    display: 'grid',
  },
  disclosure: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
  },
  disclosureSummary: {
    gap: '.5rem',
    listStyle: 'none',
    paddingBlock: '.7rem',
    paddingInline: '.8rem',
    alignItems: 'center',
    color: colors.textPrimary,
    cursor: 'pointer',
    display: 'flex',
    fontSize: typography.sizeSm,
    fontWeight: typography.weightSemibold,
  },
  summaryMeta: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontWeight: typography.weightNormal,
  },
  disclosureBody: {
    paddingInline: '.8rem',
    paddingBlockEnd: '.8rem',
    paddingBlockStart: '.7rem',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
  },
  explanation: {
    marginBlock: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    lineHeight: 1.45,
  },
  pipelineNote: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    marginBlockEnd: '.65rem',
    marginBlockStart: 0,
  },
  list: {
    margin: 0,
    padding: 0,
    gap: '.4rem',
    listStyle: 'none',
    display: 'grid',
  },
  row: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '.5rem',
    paddingInline: '.6rem',
    alignItems: 'baseline',
    columnGap: '.75rem',
    display: 'grid',
    gridTemplateColumns: 'minmax(0, 1fr) auto',
    rowGap: '.2rem',
  },
  rowTitle: {
    overflow: 'hidden',
    fontSize: typography.sizeXs,
    fontWeight: typography.weightSemibold,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  rowDetail: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    gridColumnEnd: '-1',
    gridColumnStart: '1',
  },
  rowPath: {
    overflow: 'hidden',
    color: colors.textSecondary,
    fontSize: '0.68rem',
    gridColumnEnd: '-1',
    gridColumnStart: '1',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  status: {
    borderRadius: layout.radiusPill,
    paddingBlock: '.15rem',
    paddingInline: '.4rem',
    color: colors.textSecondary,
    fontSize: '0.68rem',
    whiteSpace: 'nowrap',
  },
  statusReady: {
    backgroundColor: colors.surfaceSuccess,
    color: colors.success,
  },
  statusWarning: {
    backgroundColor: colors.surfaceWarning,
    color: colors.warning,
  },
  statusDanger: {
    backgroundColor: colors.surfaceDanger,
    color: colors.danger,
  },
  empty: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
});

function courseName(courses: CourseSummary[], courseId: string | number | undefined): string {
  const course = courses.find((item) => String(item.id) === String(courseId));
  return course?.short_name || course?.name || (courseId ? `Course ${courseId}` : 'Unknown course');
}

function statusStyle(status: string) {
  if (status === 'ready') return styles.statusReady;
  if (status === 'failed' || status === 'generation_failed') return styles.statusDanger;
  return styles.statusWarning;
}

function guideStatusLabel(status: string): string {
  return (
    {
      not_queued: 'not queued',
      pending: 'queued',
      running: 'processing',
      failed: 'failed',
      stale: 'outdated',
      ready: 'ready',
    }[status] || status.replaceAll('_', ' ')
  );
}

function guideExplanation(row: KnowledgeGuideDiagnostic): string {
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

function guideCounts(status: KnowledgeStatusPayload | null): Record<string, number> {
  return (status?.guide_summary || []).reduce<Record<string, number>>((counts, row) => {
    const key = String(row.status || 'unknown');
    counts[key] = (counts[key] || 0) + Number(row.count || 0);
    return counts;
  }, {});
}

function GuideRows({ courses, rows }: { courses: CourseSummary[]; rows: KnowledgeGuideDiagnostic[] }) {
  if (!rows.length) return <p {...stylex.props(styles.empty)}>Every indexed file currently has a guide.</p>;
  return (
    <ul {...stylex.props(styles.list)}>
      {rows.map((row) => (
        <li {...stylex.props(styles.row)} key={String(row.document_id || row.source_path)}>
          <strong title={row.display_name} {...stylex.props(styles.rowTitle)}>
            {courseName(courses, row.course_id)} · {row.display_name || row.source_path || 'Unnamed file'}
          </strong>
          <span {...stylex.props(styles.status, statusStyle(row.status || 'unknown'))}>
            {guideStatusLabel(row.status || 'unknown')}
          </span>
          <p {...stylex.props(styles.rowDetail)}>{guideExplanation(row)}</p>
          {row.source_path && row.source_path !== row.display_name ? (
            <small title={row.source_path} {...stylex.props(styles.rowPath)}>
              {row.source_path}
            </small>
          ) : null}
        </li>
      ))}
    </ul>
  );
}

function GuideDiagnostics({ courses, status }: { courses: CourseSummary[]; status: KnowledgeStatusPayload | null }) {
  const counts = guideCounts(status);
  const ready = counts.ready || 0;
  const waiting = (counts.pending || 0) + (counts.running || 0) + (counts.not_queued || 0) + (counts.stale || 0);
  const failed = counts.failed || 0;
  const rows = status?.guide_diagnostics || [];
  const enabled = status?.ai_pipeline?.enabled !== false;
  return (
    <details {...stylex.props(styles.disclosure)}>
      <summary {...stylex.props(styles.disclosureSummary)}>
        AI Guides
        <span {...stylex.props(styles.summaryMeta)}>
          {ready} ready · {waiting} waiting · {failed} failed
        </span>
      </summary>
      <div {...stylex.props(styles.disclosureBody)}>
        <p {...stylex.props(styles.pipelineNote)}>
          {enabled
            ? `Guide model: ${status?.ai_pipeline?.model || 'configured provider'}. ` +
              'Files below explain why a guide is not visible.'
            : 'AI guide generation is disabled in the current configuration.'}
        </p>
        <GuideRows courses={courses} rows={rows} />
        {status?.guide_diagnostics_truncated ? (
          <p {...stylex.props(styles.explanation)}>
            Only the first 500 files are shown; the totals above include all files.
          </p>
        ) : null}
      </div>
    </details>
  );
}

const ROADMAP_REASONS = {
  documents_pending_extraction: 'source files are still being extracted',
  extraction_jobs_active: 'source extraction jobs are active',
  document_enrichments_active: 'file guides are being generated',
  page_enrichments_active: 'visual page scans are active',
  document_enrichments_missing_current_generation: 'some files have no guide for the current model/version',
  documents_unverified_content: 'some source bytes still need verification',
  no_ready_documents: 'there are no ready source files yet',
  no_ready_document_insights: 'no file guide is ready yet',
} satisfies Record<string, string>;

function roadmapReasonLabel(reason: string): string {
  if (reason in ROADMAP_REASONS) {
    // SAFETY: the `in` check above proves this string is a declared roadmap-reason key.
    return ROADMAP_REASONS[reason as keyof typeof ROADMAP_REASONS];
  }
  return reason.replaceAll('_', ' ');
}

function roadmapExplanation(row: KnowledgeRoadmapDiagnostic): string {
  if (row.error && row.status === 'failed') return row.error;
  const reasons = row.readiness?.blocking_reasons || [];
  if (reasons.length) return reasons.map(roadmapReasonLabel).join('; ');
  if (row.status === 'pending' || row.status === 'running') return 'The roadmap generation job is queued or running.';
  if (row.status === 'missing') return 'No roadmap generation has been created yet.';
  if (row.status === 'failed')
    return 'The last roadmap generation failed; retry failed work from Knowledge maintenance.';
  return 'The roadmap is ready or waiting for a refresh.';
}

function roadmapIsReady(row: KnowledgeRoadmapDiagnostic): boolean {
  return row.status === 'ready' && row.readiness?.ready !== false;
}

function roadmapStats(readiness: KnowledgeRoadmapDiagnostic['readiness']): string {
  if (!readiness) return '';
  const docs = Number(readiness.ready_documents || 0);
  const guides = Number(readiness.ready_document_insights || 0);
  const active =
    Number(readiness.active_extraction_jobs || 0) +
    Number(readiness.active_document_enrichments || 0) +
    Number(readiness.active_page_enrichments || 0);
  return `${guides}/${docs} file guides ready${active ? ` · ${active} upstream job(s) active` : ''}`;
}

function RoadmapDiagnostics({ courses, status }: { courses: CourseSummary[]; status: KnowledgeStatusPayload | null }) {
  const rows = status?.roadmap_diagnostics || [];
  const waiting = rows.filter((row) => !roadmapIsReady(row)).length;
  return (
    <details {...stylex.props(styles.disclosure)}>
      <summary {...stylex.props(styles.disclosureSummary)}>
        Roadmaps
        <span {...stylex.props(styles.summaryMeta)}>{waiting ? `${waiting} waiting` : 'all ready'}</span>
      </summary>
      <div {...stylex.props(styles.disclosureBody)}>
        <p {...stylex.props(styles.pipelineNote)}>
          Roadmap model: {status?.ai_pipeline?.course_model || 'configured provider'}.
        </p>
        {rows.length ? (
          <ul {...stylex.props(styles.list)}>
            {rows.map((row) => (
              <li {...stylex.props(styles.row)} key={String(row.course_id)}>
                <strong {...stylex.props(styles.rowTitle)}>{courseName(courses, row.course_id)}</strong>
                <span
                  {...stylex.props(styles.status, statusStyle(roadmapIsReady(row) ? 'ready' : row.status || 'missing'))}
                >
                  {(row.status || 'missing').replaceAll('_', ' ')}
                </span>
                <p {...stylex.props(styles.rowDetail)}>{roadmapExplanation(row)}</p>
                <small {...stylex.props(styles.rowPath)}>{roadmapStats(row.readiness)}</small>
              </li>
            ))}
          </ul>
        ) : (
          <p {...stylex.props(styles.empty)}>No visible courses have roadmap state yet.</p>
        )}
      </div>
    </details>
  );
}

function sourceExplanation(row: KnowledgeSourceDiagnostic): string {
  if (row.diagnostic_reason === 'unsupported_mime_type') return 'This file type is not supported by the extractor.';
  if (row.diagnostic_reason === 'skipped_limit') return 'The indexing safety limit skipped this file.';
  return row.error || row.diagnostic_reason?.replaceAll('_', ' ') || 'The source could not be indexed.';
}

function SourceDiagnostics({ status }: { status: KnowledgeStatusPayload | null }) {
  const rows = [...(status?.failed_documents || []), ...(status?.unsupported_documents || [])];
  return (
    <details {...stylex.props(styles.disclosure)}>
      <summary {...stylex.props(styles.disclosureSummary)}>
        Source failures
        <span {...stylex.props(styles.summaryMeta)}>{rows.length ? `${rows.length} need attention` : 'none'}</span>
      </summary>
      <div {...stylex.props(styles.disclosureBody)}>
        {rows.length ? (
          <ul {...stylex.props(styles.list)}>
            {rows.map((row, index) => (
              <li {...stylex.props(styles.row)} key={`${row.course_id}-${row.source_path}-${index}`}>
                <strong {...stylex.props(styles.rowTitle)}>
                  {row.display_name || row.source_path || 'Unnamed file'}
                </strong>
                <span {...stylex.props(styles.status, statusStyle(row.status || 'failed'))}>
                  {(row.status || 'failed').replaceAll('_', ' ')}
                </span>
                <p {...stylex.props(styles.rowDetail)}>{sourceExplanation(row)}</p>
                {row.source_path ? <small {...stylex.props(styles.rowPath)}>{row.source_path}</small> : null}
              </li>
            ))}
          </ul>
        ) : (
          <p {...stylex.props(styles.empty)}>No unsupported or failed source files are reported.</p>
        )}
        {status?.diagnostics_truncated ? (
          <p {...stylex.props(styles.explanation)}>Only the first 500 source failures are shown.</p>
        ) : null}
      </div>
    </details>
  );
}

export function KnowledgeDiagnostics({
  courses,
  status,
}: {
  courses: CourseSummary[];
  status: KnowledgeStatusPayload | null;
}) {
  if (!status) return null;
  return (
    <div {...stylex.props(styles.diagnostics)}>
      <GuideDiagnostics courses={courses} status={status} />
      <RoadmapDiagnostics courses={courses} status={status} />
      <SourceDiagnostics status={status} />
    </div>
  );
}
