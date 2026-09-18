import * as stylex from '@stylexjs/stylex';
import { TriangleAlert } from 'lucide-react';
import type { ReactNode } from 'react';
import { Button } from '@/components/ui/button';
import { colors, effects, typography } from '@/styles/tokens.stylex';
import { friendlyDocumentError } from '@/features/session/reader/PdfReader';
import { errorMessage, type ErrorLike } from '@/lib/errors';

const styles = stylex.create({
  stateHost: {
    borderColor: colors.border,
    borderRadius: '0.75rem',
    borderWidth: '1px',
    placeItems: 'center',
    backgroundColor: colors.surface,
    boxShadow: effects.shadowLarge,
    display: 'grid',
    minHeight: 'min(38rem, calc(100dvh - 9rem))',
  },
  stateBody: {
    padding: '1.5rem',
    gap: '0.75rem',
    alignItems: 'center',
    color: colors.textPrimary,
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    textAlign: 'center',
    minHeight: '100%',
  },
  stateBodyError: {
    borderColor: colors.danger,
  },
  stateBodyLoading: {
    color: colors.success,
  },
  iconDestructive: {
    color: colors.danger,
  },
  iconSize: {
    height: '1.35rem',
    width: '1.35rem',
  },
  kicker: {
    color: colors.textSecondary,
    fontSize: '0.6875rem',
    fontWeight: typography.weightBold,
    letterSpacing: '0.12em',
    textTransform: 'uppercase',
  },
  title: {
    margin: 0,
    fontSize: 'clamp(1.15rem, 2vw, 1.5rem)',
    letterSpacing: '0.035em',
    maxWidth: '28rem',
  },
  copy: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    lineHeight: typography.leadingRelaxed,
    maxWidth: '28rem',
  },
  actions: {
    gap: '0.5rem',
    display: 'flex',
    flexWrap: 'wrap',
    justifyContent: 'center',
    marginTop: '0.25rem',
  },
  loadingMark: {
    gap: '0.375rem',
    alignItems: 'flex-end',
    display: 'flex',
    height: '2rem',
    marginBottom: '0.25rem',
  },
  pulseBar: {
    borderRadius: '999px',
    backgroundColor: colors.success,
    width: '0.375rem',
  },
  bar1: { height: '0.75rem' },
  bar2: { height: '1.25rem' },
  bar3: { height: '1.75rem' },
});

export interface LoadErrorProps {
  error: ErrorLike;
}

function WorkspaceStateFrame({ busy, children }: { busy: boolean; children: ReactNode }) {
  return (
    <div {...stylex.props(styles.stateHost)} aria-busy={busy} role={busy ? 'status' : undefined}>
      {children}
    </div>
  );
}

export function LoadError({ error }: LoadErrorProps) {
  return (
    <WorkspaceStateFrame busy={false}>
      <LoadErrorBody error={error} />
    </WorkspaceStateFrame>
  );
}

function LoadErrorBody({ error }: LoadErrorProps) {
  return (
    <div {...stylex.props(styles.stateBody, styles.stateBodyError)} role="alert">
      <div {...stylex.props(styles.iconDestructive)}>
        <TriangleAlert aria-hidden="true" {...stylex.props(styles.iconSize)} />
      </div>
      <span {...stylex.props(styles.kicker)}>Workspace unavailable</span>
      <h1 {...stylex.props(styles.title)}>Unable to open this study session</h1>
      <p {...stylex.props(styles.copy)}>
        {friendlyDocumentError(errorMessage(error, 'The workspace could not be opened.'))}
      </p>
      <div {...stylex.props(styles.actions)}>
        <Button variant="secondary" onClick={() => window.location.reload()}>
          Try again
        </Button>
        <Button variant="default" href="/study">
          Back to the plan
        </Button>
      </div>
    </div>
  );
}

export function LoadingState() {
  return (
    <WorkspaceStateFrame busy>
      <LoadingStateBody />
    </WorkspaceStateFrame>
  );
}

function LoadingStateBody() {
  return (
    <div {...stylex.props(styles.stateBody, styles.stateBodyLoading)}>
      <div {...stylex.props(styles.loadingMark)} aria-hidden="true">
        <span {...stylex.props(styles.pulseBar, styles.bar1)} />
        <span {...stylex.props(styles.pulseBar, styles.bar2)} />
        <span {...stylex.props(styles.pulseBar, styles.bar3)} />
      </div>
      <span {...stylex.props(styles.kicker)}>Preparing the workspace</span>
      <h1 {...stylex.props(styles.title)}>Opening the study session…</h1>
      <p {...stylex.props(styles.copy)}>Sources, notes, and progress are opening.</p>
    </div>
  );
}
