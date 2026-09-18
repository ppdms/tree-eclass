import * as stylex from '@stylexjs/stylex';
import { colors, effects, layout, typography } from '@/styles/tokens.stylex';

const pulseKeyframes = stylex.keyframes({
  '0%, 100%': { opacity: 1 },
  '50%': { opacity: 0.4 },
});

const styles = stylex.create({
  sessionStateHost: {
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    placeItems: 'center',
    backgroundColor: colors.surface,
    boxShadow: effects.shadowLarge,
    display: 'grid',
    minHeight: 'min(38rem, calc(100dvh - 9rem))',
  },
  sessionState: {
    padding: '1.5rem',
    gap: '.75rem',
    alignItems: 'center',
    color: colors.success,
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    textAlign: 'center',
    minHeight: '100%',
  },
  sessionStateKicker: {
    color: colors.textSecondary,
    fontSize: '0.6875rem',
    fontWeight: typography.weightBold,
    letterSpacing: '.12em',
    textTransform: 'uppercase',
  },
  sessionStateTitle: {
    margin: 0,
    color: colors.textPrimary,
    fontSize: 'clamp(1.15rem, 2vw, 1.5rem)',
    letterSpacing: '.035em',
    maxWidth: '28rem',
  },
  sessionStateDescription: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    lineHeight: typography.leadingRelaxed,
    maxWidth: '28rem',
  },
  sessionLoadingMark: {
    gap: '.375rem',
    alignItems: 'flex-end',
    display: 'flex',
    height: '2rem',
    marginBottom: '.25rem',
  },
  bar1: {
    borderRadius: '999px',
    animationDuration: '1.4s',
    animationIterationCount: 'infinite',
    animationName: pulseKeyframes,
    backgroundColor: colors.success,
    height: '.75rem',
    width: '3.75rem',
  },
  bar2: {
    borderRadius: '999px',
    animationDelay: '0.12s',
    animationDuration: '1.4s',
    animationIterationCount: 'infinite',
    animationName: pulseKeyframes,
    backgroundColor: colors.success,
    height: '1.25rem',
    width: '3.75rem',
  },
  bar3: {
    borderRadius: '999px',
    animationDelay: '0.24s',
    animationDuration: '1.4s',
    animationIterationCount: 'infinite',
    animationName: pulseKeyframes,
    backgroundColor: colors.success,
    height: '1.75rem',
    width: '3.75rem',
  },
});

export function SessionLoading() {
  return (
    <div {...stylex.props(styles.sessionStateHost)} aria-busy="true" role="status">
      <SessionLoadingBody />
    </div>
  );
}

function SessionLoadingBody() {
  return (
    <div {...stylex.props(styles.sessionState)}>
      <SessionLoadingMark />
      <span {...stylex.props(styles.sessionStateKicker)}>Preparing the workspace</span>
      <h1 {...stylex.props(styles.sessionStateTitle)}>Opening the study session…</h1>
      <p {...stylex.props(styles.sessionStateDescription)}>Sources, notes, and progress are opening.</p>
    </div>
  );
}

function SessionLoadingMark() {
  return (
    <div {...stylex.props(styles.sessionLoadingMark)} aria-hidden="true">
      <span {...stylex.props(styles.bar1)} />
      <span {...stylex.props(styles.bar2)} />
      <span {...stylex.props(styles.bar3)} />
    </div>
  );
}
