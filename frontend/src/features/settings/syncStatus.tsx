import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { fetchJson } from '@/lib/api';
import type { SyncStatusEntry, SyncStatusPayload } from './types';
import { colors, typography } from '@/styles/tokens.stylex';
import { formatDateTime, isSuccessfulResult } from './format';

const styles = stylex.create({
  settingsSyncStatusLine: {
    margin: 0,
    fontSize: typography.sizeSm,
  },
  settingsSyncStatusOk: {
    color: colors.success,
  },
  settingsSyncStatusFailed: {
    color: colors.danger,
  },
  isFailed: {
    color: colors.danger,
  },
});

const SyncContext = React.createContext<SyncStatusPayload | null>(null);

export function SyncStatusProvider({ children }: { children: React.ReactNode }) {
  const [status, setStatus] = React.useState<SyncStatusPayload | null>(null);
  React.useEffect(() => {
    const controller = new AbortController();
    fetchJson<SyncStatusPayload>('api/settings/sync-status', { signal: controller.signal })
      .then((value) => {
        if (!controller.signal.aborted) setStatus(value);
      })
      .catch(() => {});
    return () => controller.abort();
  }, []);
  return <SyncContext.Provider value={status}>{children}</SyncContext.Provider>;
}

export function useSyncStatus() {
  return React.useContext(SyncContext);
}

export interface LastRunLineProps {
  job: string;
  label: string;
}

export function LastRunLine({ job, label }: LastRunLineProps) {
  const status = useSyncStatus();
  const row: SyncStatusEntry | null = status?.sync?.[job] || null;
  if (!row?.last_run_at) return null;
  const ok = isSuccessfulResult(row.last_result);
  return (
    <p
      {...stylex.props(
        styles.settingsSyncStatusLine,
        ok ? styles.settingsSyncStatusOk : styles.settingsSyncStatusFailed,
      )}
    >
      Last {label}: {formatDateTime(row.last_run_at)} · <strong>{ok ? 'succeeded' : row.last_result}</strong>
      {row.last_error ? ` — ${row.last_error}` : ''}
    </p>
  );
}

export function CheckFailureSignal() {
  const check = useSyncStatus()?.check || null;
  if (!check?.last_error) return null;
  return (
    <p {...stylex.props(styles.settingsSyncStatusLine, styles.isFailed)} role="alert">
      Last check failed: {check.last_error}
    </p>
  );
}
