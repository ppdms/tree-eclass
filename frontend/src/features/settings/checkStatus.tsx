import { useCheckPolling } from './useCheckPolling';
import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { buttonStyles } from '@/components/ui/styles';
import { fetchJson } from '@/lib/api';
import { errorLikeSchema, errorMessage } from '@/lib/errors';
import type { CheckStatusPayload } from './types';
import { colors, layout, typography } from '@/styles/tokens.stylex';
import { formatDateTime, isSuccessfulResult } from './format';

const styles = stylex.create({
  settingsCheckSection: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'dashed',
    borderWidth: 1,
    paddingBlock: '1rem',
    paddingInline: '1.25rem',
    backgroundColor: colors.surfaceRaised,
    marginTop: '1rem',
  },
  sectionTitle: {
    margin: 0,
    fontSize: '1rem',
    marginBottom: '0.25rem',
  },
  sectionDesc: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
  },
  jobStatus: {
    alignItems: 'center',
    color: colors.info,
    columnGap: '1rem',
    display: 'flex',
    flexWrap: 'wrap',
    fontSize: typography.sizeSm,
    rowGap: '0.65rem',
    marginTop: '0.75rem',
  },
  settingsCheckStatusLine: {
    margin: 0,
    fontSize: typography.sizeSm,
  },
  settingsCheckStatusOk: {
    color: colors.success,
  },
  isFailed: {
    color: colors.danger,
  },
});

interface UseCheckStatusResult {
  status: CheckStatusPayload | null;
  error: string | null;
  pollingError: string | null;
  busy: boolean;
  start: () => Promise<void>;
}

interface StartCheckArgs {
  busy: boolean;
  setBusy: (busy: boolean) => void;
  setError: (error: string | null) => void;
  setStatus: React.Dispatch<React.SetStateAction<CheckStatusPayload | null>>;
  load: () => Promise<CheckStatusPayload | null>;
}

async function startCheck(args: StartCheckArgs): Promise<void> {
  const { busy, setBusy, setError, setStatus, load } = args;
  if (busy) return;
  setBusy(true);
  setError(null);
  try {
    const result: { current_course?: string } = await fetchJson('api/run-check', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    setStatus({ is_checking: true, course_name: result.current_course });
    load();
  } catch (startError) {
    const parsed = errorLikeSchema.safeParse(startError);
    const errorLike = parsed.success ? parsed.data : { message: String(startError) };
    if (errorLike.status === 409) {
      setStatus((current) => ({ ...current, is_checking: true }));
      setError('A course check is already in progress.');
      load();
    } else {
      setError(errorMessage(errorLike, 'Could not start the course check.'));
    }
  } finally {
    setBusy(false);
  }
}

function useCheckStatus(): UseCheckStatusResult {
  const { status, setStatus, pollingError, load } = useCheckPolling();
  const [error, setError] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);
  const start = React.useCallback(() => startCheck({ busy, setBusy, setError, setStatus, load }), [busy, load]);
  return { status, error, pollingError, busy, start };
}

function CheckStatusDetails({
  status,
  busy,
  start,
  counts,
  visibleError,
}: {
  status: CheckStatusPayload | null;
  busy: boolean;
  start: () => Promise<void>;
  counts: string;
  visibleError: string | null | undefined;
}) {
  return (
    <div {...stylex.props(styles.jobStatus)} role="status" aria-live="polite">
      {status?.is_checking ? (
        <span>Course check in progress{status.course_name ? ` · ${status.course_name}` : ''}…</span>
      ) : (
        <button
          type="button"
          {...stylex.props(buttonStyles.base, buttonStyles.secondary)}
          onClick={start}
          disabled={busy}
        >
          {busy ? 'Starting…' : 'Run course check'}
        </button>
      )}
      <CheckStatusMessages status={status} counts={counts} visibleError={visibleError} />
    </div>
  );
}

function CheckStatusMessages({
  status,
  counts,
  visibleError,
}: Pick<Parameters<typeof CheckStatusDetails>[0], 'status' | 'counts' | 'visibleError'>) {
  return (
    <>
      {status?.last_check_at && !status?.is_checking && (
        <p
          {...stylex.props(
            styles.settingsCheckStatusLine,
            isSuccessfulResult(status.last_check_result) ? styles.settingsCheckStatusOk : styles.isFailed,
          )}
        >
          Last check: {formatDateTime(status.last_check_at)} ·{' '}
          <strong>{isSuccessfulResult(status.last_check_result) ? 'succeeded' : 'failed'}</strong>
          {counts}
        </p>
      )}
      {visibleError && (
        <p {...stylex.props(styles.settingsCheckStatusLine, styles.isFailed)} role="alert">
          {visibleError}
        </p>
      )}
    </>
  );
}

export function CheckStatus() {
  const { status, error, pollingError, busy, start } = useCheckStatus();
  const counts =
    status && (status.last_files_added || status.last_files_changed)
      ? ` · ${status.last_files_added ?? 0} file change(s), ${status.last_files_changed ?? 0} exercise event(s)`
      : '';
  const visibleError = error || pollingError || status?.last_error;
  return (
    <section {...stylex.props(styles.settingsCheckSection)} aria-labelledby="settings-check-title">
      <h2 id="settings-check-title" {...stylex.props(styles.sectionTitle)}>
        Course sync
      </h2>
      <p {...stylex.props(styles.sectionDesc)}>
        Refresh course files, announcements, and study evidence from the university source.
      </p>
      <CheckStatusDetails status={status} busy={busy} start={start} counts={counts} visibleError={visibleError} />
    </section>
  );
}
