import { useRef, useState } from 'react';
import type { KnowledgeStatusPayload, KnowledgeStatusRow } from '@/features/settings/types';
import { requestJson } from '../app/api';
import { Button, Notice, Progress } from '../ui/controls';
import { Dialog } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { Disclosure, Section } from './section';
import { KnowledgeDiagnostics } from './diagnostics';
import { coverageState, knowledgeTotals } from './knowledgeModel';
import { useKnowledgeSummary, usePollResource } from './resources';
import { Hint, message, styles, useSettingsContext, type SettingsCourse } from './shared';

type MaintenanceAction = 'retry-failed' | 'reconcile' | 'rebuild';
const actionLabels = {
  'retry-failed': 'Retry',
  reconcile: 'Source reconciliation',
  rebuild: 'Index rebuild',
} satisfies Record<MaintenanceAction, string>;
function KnowledgeTotals({ status }: { status: KnowledgeStatusPayload | null }) {
  const totals = knowledgeTotals(status);
  const values = [
    ['indexed', totals.indexed],
    ['not indexed', Math.max(0, totals.supported - totals.indexed)],
    ['pending', totals.pending],
    ['failed', totals.failed],
  ] as const;
  return (
    <View style={styles.row}>
      {values.map(([label, value]) => (
        <View key={label} style={styles.card}>
          <Text style={styles.heading}>{value}</Text>
          <Hint>{label}</Hint>
        </View>
      ))}
    </View>
  );
}
function CourseCoverage({ courses, status }: { courses: SettingsCourse[]; status: KnowledgeStatusPayload | null }) {
  const totals = knowledgeTotals(status);
  const rows = new Map<string, KnowledgeStatusRow>((status?.coverage ?? []).map((row) => [String(row.course_id), row]));
  return (
    <Disclosure id="settings-source-index" title={`Source index · ${totals.indexed}/${totals.supported} indexed`}>
      {courses.map((course) => {
        const row = rows.get(String(course.id)),
          coverage = coverageState(row);
        const empty = !row || !coverage.supported;
        return (
          <View key={course.id} style={styles.card}>
            <Text title={course.name} style={styles.heading}>
              {course.short_name || course.name}
            </Text>
            <Text>{empty ? 'No sources' : `${coverage.percent}%`}</Text>
            <Progress
              id={`settings-coverage-${course.id}`}
              value={coverage.percent}
              max={100}
              label={`${coverage.indexed}/${coverage.supported} indexed`}
            />
            {!empty && coverage.state !== 'ready' && (
              <Notice tone={coverage.failed ? 'danger' : 'warning'}>
                {coverage.failed ? `${coverage.failed} failed` : `${coverage.pending} pending`}
              </Notice>
            )}
          </View>
        );
      })}
    </Disclosure>
  );
}
function MaintenanceActions({
  busy,
  onRun,
  onRebuild,
}: {
  busy: MaintenanceAction | null;
  onRun(action: MaintenanceAction): void;
  onRebuild(): void;
}) {
  return (
    <View style={styles.row}>
      <Button id="settings-retry-failed" disabled={Boolean(busy)} onClick={() => onRun('retry-failed')}>
        Retry failed
      </Button>
      <Button id="settings-reconcile" disabled={Boolean(busy)} onClick={() => onRun('reconcile')}>
        Reconcile sources
      </Button>
      <Button id="settings-rebuild" disabled={Boolean(busy)} onClick={onRebuild}>
        Rebuild index
      </Button>
    </View>
  );
}
function RebuildDialog({ open, onClose, onConfirm }: { open: boolean; onClose(): void; onConfirm(): void }) {
  return (
    <Dialog
      id="settings-rebuild-dialog"
      title="Rebuild the knowledge index?"
      open={open}
      onClose={onClose}
      actions={
        <>
          <Button id="settings-rebuild-cancel" onClick={onClose}>
            Cancel
          </Button>
          <Button id="settings-rebuild-confirm" onClick={onConfirm}>
            Rebuild index
          </Button>
        </>
      }
    >
      <Text>This recalculates the local search index and may keep Ask unavailable while it runs.</Text>
    </Dialog>
  );
}
function MaintenanceControls({ onRefresh }: { onRefresh(): void }) {
  const [busy, setBusy] = useState<MaintenanceAction | null>(null);
  const pending = useRef(false);
  const [feedback, setFeedback] = useState<{ text: string; error: boolean } | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const run = async (action: MaintenanceAction) => {
    if (pending.current) return;
    pending.current = true;
    setBusy(action);
    setFeedback(null);
    try {
      await requestJson(`/api/knowledge/${action}`, { method: 'POST' });
      setFeedback({ text: `${actionLabels[action]} queued.`, error: false });
      onRefresh();
    } catch (failure) {
      setFeedback({ text: message(failure, 'Knowledge maintenance failed.'), error: true });
    } finally {
      pending.current = false;
      setBusy(null);
    }
  };
  return (
    <>
      <MaintenanceActions busy={busy} onRun={(action) => void run(action)} onRebuild={() => setConfirmOpen(true)} />
      {busy && <Notice tone="info">Queuing knowledge maintenance…</Notice>}
      {feedback && <Notice tone={feedback.error ? 'danger' : 'success'}>{feedback.text}</Notice>}
      <RebuildDialog
        open={confirmOpen}
        onClose={() => setConfirmOpen(false)}
        onConfirm={() => {
          setConfirmOpen(false);
          void run('rebuild');
        }}
      />
    </>
  );
}
export function KnowledgeSection({ courses }: { courses: SettingsCourse[] }) {
  const context = useSettingsContext();
  const summary = useKnowledgeSummary();
  const details = usePollResource<KnowledgeStatusPayload>(
    '/api/knowledge/status',
    30000,
    context.opened.has('knowledge'),
  );
  const totals = knowledgeTotals(summary.data),
    notIndexed = Math.max(0, totals.supported - totals.indexed);
  const status = !summary.data
    ? summary.error
      ? 'Summary unavailable'
      : 'Opening summary…'
    : totals.failed
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
      advanced
      status={status}
      description="Inspect source coverage and repair indexing work without leaving Settings."
    >
      {!summary.data && summary.error && <Notice tone="danger">{summary.error}</Notice>}
      <KnowledgeTotals status={summary.data} />
      {details.error && !details.data ? (
        <Notice tone="danger">{details.error}</Notice>
      ) : (
        <KnowledgeDiagnostics courses={courses} status={details.data} />
      )}
      <CourseCoverage courses={courses} status={summary.data} />
      <MaintenanceControls onRefresh={summary.refresh} />
      <Hint>Queued maintenance is background work; these acknowledgements do not mean indexing has finished.</Hint>
    </Section>
  );
}
