import * as stylex from '@stylexjs/stylex';
import { knowledgeStyles } from './knowledgeStyles';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import { fetchJson } from '@/lib/api';
import type { CourseSummary } from '@/lib/types';
import type { KnowledgeStatusPayload } from './types';
import { Section } from './settingsForm';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import { KnowledgeDiagnostics } from './knowledgeDiagnostics';
import { useKnowledgeResource } from './useKnowledgeResource';

const styles = knowledgeStyles;

type KnowledgeStatusRow = NonNullable<KnowledgeStatusPayload['coverage']>[number];

interface KnowledgeTotals {
  supported: number;
  indexed: number;
  failed: number;
  pending: number;
}

function knowledgeTotals(status: KnowledgeStatusPayload | null): KnowledgeTotals {
  const coverage = status?.coverage || [];
  return coverage.reduce<KnowledgeTotals>(
    (totals, row) => {
      const supported = totals.supported + Number(row.supported_documents || 0);
      const indexed = totals.indexed + Number(row.indexed_documents || 0);
      return {
        supported,
        indexed,
        failed: totals.failed + Number(row.failed_documents || 0),
        pending: totals.pending + Number(row.pending_documents || 0),
      };
    },
    { supported: 0, indexed: 0, failed: 0, pending: 0 },
  );
}

type KnowledgeAction = 'retry-failed' | 'reconcile' | 'rebuild';

const actionLabels = {
  'retry-failed': 'Retry',
  reconcile: 'Source reconciliation',
  rebuild: 'Index rebuild',
} satisfies Record<KnowledgeAction, string>;

interface KnowledgeMessage {
  type: 'status' | 'error';
  text: string;
}

function useKnowledgeMaintenance() {
  const { status, refresh: load } = useKnowledgeResource('api/knowledge/summary');
  const [busy, setBusy] = React.useState<KnowledgeAction | null>(null);
  const [message, setMessage] = React.useState<KnowledgeMessage | null>(null);
  const [confirmOpen, setConfirmOpen] = React.useState(false);
  const run = async (action: KnowledgeAction) => {
    setBusy(action);
    setMessage(null);
    try {
      await fetchJson(`api/knowledge/${action}`, { method: 'POST' });
      const detail = `${actionLabels[action]} queued.`;
      setMessage({ type: 'status', text: detail });
      load();
    } catch (error) {
      setMessage({
        type: 'error',
        text: error instanceof Error ? error.message : 'Knowledge maintenance failed.',
      });
    } finally {
      setBusy(null);
    }
  };
  return { status, busy, message, confirmOpen, setConfirmOpen, run };
}

export interface KnowledgeTotalsProps {
  totals: KnowledgeTotals;
}

function KnowledgeTotalTile({ value, label }: { value: number; label: string }) {
  return (
    <div {...stylex.props(styles.totalTile)}>
      <strong {...stylex.props(styles.tileValue)}>{value}</strong>
      <span {...stylex.props(styles.tileLabel)}>{label}</span>
    </div>
  );
}

export function KnowledgeTotals({ totals }: KnowledgeTotalsProps) {
  const notIndexed = Math.max(0, totals.supported - totals.indexed);
  return (
    <div {...stylex.props(styles.settingsKnowledgeSummary)} aria-live="polite">
      <KnowledgeTotalTile value={totals.indexed} label="indexed" />
      <KnowledgeTotalTile value={notIndexed} label="not indexed" />
      <KnowledgeTotalTile value={totals.pending} label="pending" />
      <KnowledgeTotalTile value={totals.failed} label="failed" />
    </div>
  );
}

function coverageState(row: KnowledgeStatusRow | undefined) {
  const supported = Number(row?.supported_documents || 0);
  const indexed = Number(row?.indexed_documents || 0);
  const failed = Number(row?.failed_documents || 0);
  const pending = Number(row?.pending_documents || 0);
  const percent = supported ? Math.round((indexed / supported) * 100) : 0;
  const state = failed ? 'failed' : pending || percent < 100 ? 'pending' : 'ready';
  return { supported, indexed, failed, pending, percent, state };
}

function courseCoverageStyle(state: string) {
  if (state === 'failed') return 'settingsCourseIndexFailed' as const;
  if (state === 'pending') return 'settingsCourseIndexPending' as const;
  return 'settingsCourseIndexReady' as const;
}

function CourseCoverageRow({ course, row }: { course: CourseSummary; row?: KnowledgeStatusRow }) {
  const coverage = coverageState(row);
  const empty = !row || !coverage.supported;
  const isFailed = coverage.state === 'failed';
  const isPending = coverage.state === 'pending';
  const progressStyle = isFailed
    ? styles.progressFillDanger
    : isPending
      ? styles.progressFillWarning
      : styles.progressFillSuccess;
  return (
    <div
      {...stylex.props(
        styles.settingsCourseIndex,
        empty
          ? styles.settingsCourseIndexEmpty
          : courseCoverageStyle(coverage.state) === 'settingsCourseIndexFailed'
            ? styles.settingsCourseIndexFailed
            : courseCoverageStyle(coverage.state) === 'settingsCourseIndexPending'
              ? styles.settingsCourseIndexPending
              : styles.settingsCourseIndexReady,
      )}
    >
      <span title={course.name} {...stylex.props(styles.courseName)}>
        {course.short_name || course.name}
      </span>
      <strong {...stylex.props(styles.percentText)}>{empty ? 'No sources' : `${coverage.percent}%`}</strong>
      <i aria-hidden="true" {...stylex.props(styles.progressBar)}>
        <b {...stylex.props(styles.progressFill(`${coverage.percent}%`), progressStyle)} />
      </i>
      {!empty && coverage.state !== 'ready' ? (
        <small {...stylex.props(isFailed ? styles.statusSmallDanger : styles.statusSmall)}>
          {coverage.failed ? `${coverage.failed} failed` : `${coverage.pending} pending`}
        </small>
      ) : null}
    </div>
  );
}

function CourseCoverage({ courses, status }: { courses: CourseSummary[]; status: KnowledgeStatusPayload | null }) {
  const coverage = status?.coverage || [];
  const indexed = coverage.reduce((total, row) => total + Number(row.indexed_documents || 0), 0);
  const supported = coverage.reduce((total, row) => total + Number(row.supported_documents || 0), 0);
  return (
    <details {...stylex.props(styles.settingsCourseCoverage)}>
      <summary {...stylex.props(styles.coverageSummary)}>
        <span>Source index</span>
        <span {...stylex.props(styles.coverageSummaryMeta)}>
          {indexed}/{supported} indexed
        </span>
      </summary>
      <div {...stylex.props(styles.coverageBody)}>
        <div {...stylex.props(styles.courseCoverageGrid)}>
          {courses.map((course) => (
            <CourseCoverageRow
              course={course}
              row={coverage.find((row) => String(row.course_id) === String(course.id))}
              key={String(course.id)}
            />
          ))}
        </div>
      </div>
    </details>
  );
}

export interface MaintenanceActionsProps {
  busy: KnowledgeAction | null;
  onRun: (action: KnowledgeAction) => void;
  onRebuild: () => void;
}

export function MaintenanceActions({ busy, onRun, onRebuild }: MaintenanceActionsProps) {
  return (
    <div {...stylex.props(styles.settingsMaintenanceActions)}>
      <button
        type="button"
        {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
        onClick={() => onRun('retry-failed')}
        disabled={Boolean(busy)}
      >
        Retry failed
      </button>
      <button
        type="button"
        {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
        onClick={() => onRun('reconcile')}
        disabled={Boolean(busy)}
      >
        Reconcile sources
      </button>
      <button
        type="button"
        {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
        onClick={onRebuild}
        disabled={Boolean(busy)}
      >
        Rebuild index
      </button>
    </div>
  );
}

export interface KnowledgeMessageProps {
  busy: KnowledgeAction | null;
  message: KnowledgeMessage | null;
}

export function KnowledgeMessage({ busy, message }: KnowledgeMessageProps) {
  if (!message) return null;
  return (
    <p
      {...stylex.props(message.type === 'error' ? styles.settingsFormError : styles.settingsFormSuccess)}
      role={message.type === 'error' ? 'alert' : 'status'}
    >
      {busy ? 'Working…' : message.text}
    </p>
  );
}

export function KnowledgeMaintenance({ courses }: { courses: CourseSummary[] }) {
  const { status, busy, message, confirmOpen, setConfirmOpen, run } = useKnowledgeMaintenance();
  const [expanded, setExpanded] = React.useState(false);
  const details = useKnowledgeResource('api/knowledge/status', expanded);
  const totals = knowledgeTotals(status);
  const notIndexed = Math.max(0, totals.supported - totals.indexed);
  const statusLabel = totals.failed
    ? `${totals.failed} failed`
    : totals.pending
      ? `${totals.pending} pending`
      : notIndexed
        ? `${notIndexed} not indexed`
        : 'Operational';
  return (
    <Section
      id="knowledge"
      title="Knowledge maintenance"
      collapsible
      defaultOpen={false}
      onOpenChange={setExpanded}
      status={statusLabel}
      description="Inspect source coverage and repair indexing work without leaving Settings."
    >
      <KnowledgeTotals totals={totals} />
      <KnowledgeDiagnostics courses={courses} status={details.status} />
      <CourseCoverage courses={courses} status={status} />
      <MaintenanceActions busy={busy} onRun={run} onRebuild={() => setConfirmOpen(true)} />
      <KnowledgeMessage busy={busy} message={message} />
      <ConfirmDialog
        open={confirmOpen}
        title="Rebuild the knowledge index?"
        description="This recalculates the local search index and may keep Ask unavailable while it runs."
        confirmLabel="Rebuild index"
        onCancel={() => setConfirmOpen(false)}
        onConfirm={() => {
          setConfirmOpen(false);
          run('rebuild');
        }}
      />
    </Section>
  );
}
