import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import { buttonStyles } from '@/components/ui/styles';
import { fetchJson } from '@/lib/api';
import { colors, spacing, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  studyEventStatus: {
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    marginLeft: spacing.sm,
  },
  statusError: {
    color: colors.danger,
  },
});

export function RunCheckButton() {
  const [open, setOpen] = React.useState(false);
  const [state, setState] = React.useState<string | null>(null);
  const run = async () => {
    setOpen(false);
    setState('Starting course check…');
    try {
      await fetchJson('api/run-check', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });
      setState('Course check started. Study guidance will refresh when it finishes.');
    } catch (error) {
      setState(error instanceof Error ? error.message : 'Could not start the course check.');
    }
  };
  return (
    <>
      <button type="button" {...stylex.props(buttonStyles.base, buttonStyles.secondary)} onClick={() => setOpen(true)}>
        Run check
      </button>
      {state && (
        <span
          {...stylex.props(styles.studyEventStatus, state.startsWith('Could') && styles.statusError)}
          role={state.startsWith('Could') ? 'alert' : 'status'}
        >
          {state}
        </span>
      )}
      <ConfirmDialog
        open={open}
        title="Run course check?"
        description="Refresh course files, announcements, and study evidence now. This can take a few minutes."
        confirmLabel="Run check"
        onCancel={() => setOpen(false)}
        onConfirm={run}
      />
    </>
  );
}
