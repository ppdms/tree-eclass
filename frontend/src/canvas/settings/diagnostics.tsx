import type { KnowledgeStatusPayload, KnowledgeGuideDiagnostic } from '@/features/settings/types';
import { lookup } from '@/lib/display';
import { Notice, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { Disclosure } from './section';
import { Hint, styles, type SettingsCourse } from './shared';
import {
  courseLabel,
  guideExplanation,
  guideStatusLabels,
  roadmapExplanation,
  roadmapStats,
  sourceExplanation,
} from './knowledgeModel';

function GuideRow({ courses, row }: { courses: SettingsCourse[]; row: KnowledgeGuideDiagnostic }) {
  const status = row.status || 'unknown';
  return (
    <Panel>
      <Text style={styles.heading}>
        {`${courseLabel(courses, row.course_id)} · ${row.display_name || row.source_path || 'Unnamed file'}`}
      </Text>
      <Notice tone={status === 'ready' ? 'success' : status === 'failed' ? 'danger' : 'warning'}>
        {lookup(guideStatusLabels, status, status.replaceAll('_', ' '))}
      </Notice>
      <Text style={styles.text}>{guideExplanation(row)}</Text>
      {row.source_path && row.source_path !== row.display_name && (
        <Text title={row.source_path} style={styles.muted}>
          {row.source_path}
        </Text>
      )}
    </Panel>
  );
}
function GuideDiagnostics({ courses, status }: { courses: SettingsCourse[]; status: KnowledgeStatusPayload }) {
  const counts: Record<string, number> = {};
  for (const row of status.guide_summary ?? []) {
    const key = row.status || 'unknown';
    counts[key] = (counts[key] || 0) + Number(row.count || 0);
  }
  const waiting = (counts.pending || 0) + (counts.running || 0) + (counts.not_queued || 0) + (counts.stale || 0);
  const rows = status.guide_diagnostics ?? [];
  return (
    <Disclosure
      id="settings-guides"
      title={`AI Guides · ${counts.ready || 0} ready · ` + `${waiting} waiting · ${counts.failed || 0} failed`}
    >
      <Hint>
        {status.ai_pipeline?.enabled !== false
          ? `Guide model: ${status.ai_pipeline?.model || 'configured provider'}. Files below explain missing guides.`
          : 'AI guide generation is disabled in the current configuration.'}
      </Hint>
      {rows.length ? (
        rows.map((row) => (
          <GuideRow
            key={JSON.stringify([row.course_id, row.document_id, row.source_path])}
            courses={courses}
            row={row}
          />
        ))
      ) : (
        <Hint>Every indexed file currently has a guide.</Hint>
      )}
      {status.guide_diagnostics_truncated && (
        <Hint>Only the first 500 files are shown; the totals above include all files.</Hint>
      )}
    </Disclosure>
  );
}
function RoadmapDiagnostics({ courses, status }: { courses: SettingsCourse[]; status: KnowledgeStatusPayload }) {
  const rows = status.roadmap_diagnostics ?? [];
  const waiting = rows.filter((row) => !(row.status === 'ready' && row.readiness?.ready !== false)).length;
  return (
    <Disclosure id="settings-roadmaps" title={`Roadmaps · ${waiting ? `${waiting} waiting` : 'all ready'}`}>
      <Hint>{`Roadmap model: ${status.ai_pipeline?.course_model || 'configured provider'}.`}</Hint>
      {rows.length ? (
        rows.map((row) => {
          const ready = row.status === 'ready' && row.readiness?.ready !== false;
          return (
            <Panel key={String(row.course_id)}>
              <Text style={styles.heading}>{courseLabel(courses, row.course_id)}</Text>
              <Notice tone={ready ? 'success' : row.status === 'failed' ? 'danger' : 'warning'}>
                {(row.status || 'missing').replaceAll('_', ' ')}
              </Notice>
              <Text style={styles.text}>{roadmapExplanation(row)}</Text>
              <Hint>{roadmapStats(row.readiness)}</Hint>
            </Panel>
          );
        })
      ) : (
        <Hint>No visible courses have roadmap state yet.</Hint>
      )}
    </Disclosure>
  );
}
function SourceDiagnostics({ status }: { status: KnowledgeStatusPayload }) {
  const rows = [...(status.failed_documents ?? []), ...(status.unsupported_documents ?? [])];
  return (
    <Disclosure
      id="settings-source-failures"
      title={`Source failures · ${rows.length ? `${rows.length} need attention` : 'none'}`}
    >
      {rows.length ? (
        rows.map((row) => (
          <Panel
            key={JSON.stringify([row.course_id, row.source_path, row.status, row.document_kind, row.diagnostic_reason])}
          >
            <Text style={styles.heading}>{row.display_name || row.source_path || 'Unnamed file'}</Text>
            <Notice tone="danger">{(row.status || 'failed').replaceAll('_', ' ')}</Notice>
            <Text style={styles.text}>{sourceExplanation(row)}</Text>
            {row.source_path && (
              <Text title={row.source_path} style={styles.muted}>
                {row.source_path}
              </Text>
            )}
          </Panel>
        ))
      ) : (
        <Hint>No unsupported or failed source files are reported.</Hint>
      )}
      {status.diagnostics_truncated && <Hint>Only the first 500 source failures are shown.</Hint>}
    </Disclosure>
  );
}
export function KnowledgeDiagnostics({
  courses,
  status,
}: {
  courses: SettingsCourse[];
  status: KnowledgeStatusPayload | null;
}) {
  if (!status) return <Hint>Opening knowledge diagnostics…</Hint>;
  return (
    <View style={styles.stack}>
      <GuideDiagnostics courses={courses} status={status} />
      <RoadmapDiagnostics courses={courses} status={status} />
      <SourceDiagnostics status={status} />
    </View>
  );
}
