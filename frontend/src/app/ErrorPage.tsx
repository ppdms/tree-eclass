import * as stylex from '@stylexjs/stylex';
import { Inline, Stack } from '@/components/ui/layout';
import { buttonStyles } from '@/components/ui/styles';
import { colors, layout, typography } from '@/styles/tokens.stylex';

export interface PageError {
  title: string;
  message: string;
  recovery: string;
  technicalDetail?: string;
}

const styles = stylex.create({
  appErrorSurface: {
    padding: '2rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    marginBlock: '4rem',
    marginInline: 'auto',
    backgroundColor: colors.surface,
    maxWidth: '42rem',
  },
  appErrorKicker: {
    color: colors.danger,
    fontSize: '0.6875rem',
    fontWeight: typography.weightBold,
    letterSpacing: '.1em',
    textTransform: 'uppercase',
  },
  appErrorTitle: {
    marginBlock: '.5rem',
    marginInline: 0,
    fontSize: layout.pageTitle,
    lineHeight: 1.1,
  },
  appErrorMessage: {
    color: colors.textSecondary,
    maxWidth: '65ch',
  },
  appErrorRecovery: {
    marginTop: '.75rem',
  },
  appErrorDetails: {
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    marginTop: '.75rem',
  },
  appErrorActions: {
    gap: '.5rem',
    display: 'flex',
    flexWrap: 'wrap',
    marginTop: '1.25rem',
  },
});

export function ErrorPage({
  error,
  onRetry,
  technicalDetail,
}: {
  error: PageError;
  onRetry?: () => void;
  technicalDetail?: string;
}) {
  return (
    <section {...stylex.props(styles.appErrorSurface)} role="alert" aria-labelledby="app-error-title">
      <Stack gap="lg">
        <ErrorCopy error={error} technicalDetail={technicalDetail} />
        <ErrorActions onRetry={onRetry} />
      </Stack>
    </section>
  );
}

function ErrorCopy({ error, technicalDetail }: Pick<Parameters<typeof ErrorPage>[0], 'error' | 'technicalDetail'>) {
  return (
    <Stack gap="sm">
      <p {...stylex.props(styles.appErrorKicker)}>tree-eClass</p>
      <h1 id="app-error-title" {...stylex.props(styles.appErrorTitle)}>
        {error.title}
      </h1>
      <p {...stylex.props(styles.appErrorMessage)}>{error.message}</p>
      <p {...stylex.props(styles.appErrorRecovery)}>{error.recovery}</p>
      {technicalDetail && (
        <details {...stylex.props(styles.appErrorDetails)}>
          <summary>Show technical details</summary>
          <code>{technicalDetail}</code>
        </details>
      )}
    </Stack>
  );
}

function ErrorActions({ onRetry }: { onRetry?: () => void }) {
  return (
    <Inline gap="sm" style={styles.appErrorActions}>
      <button
        {...stylex.props(buttonStyles.base, buttonStyles.primary)}
        type="button"
        onClick={onRetry || (() => window.location.reload())}
      >
        Try again
      </button>
      <a {...stylex.props(buttonStyles.base, buttonStyles.secondary)} href="/">
        Return to Activity
      </a>
      <a {...stylex.props(buttonStyles.base, buttonStyles.secondary)} href="/study">
        Open Study
      </a>
    </Inline>
  );
}
