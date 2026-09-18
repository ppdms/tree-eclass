import * as stylex from '@stylexjs/stylex';
import { ArrowUpRight } from 'lucide-react';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  askExamples: {
    gap: '0.65rem',
    display: 'flex',
    flexDirection: 'column',
    marginTop: '1.5rem',
  },
  askExamplesHeading: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontWeight: 650,
    letterSpacing: '0.04em',
    textAlign: 'center',
    textTransform: 'uppercase',
  },
  askExamplesList: {
    gap: '0.55rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: 'minmax(0, 1fr)',
    },
  },
  askExampleBtn: {
    borderColor: { default: colors.border, ':hover:not(:disabled)': colors.borderLight },
    borderRadius: '0.7rem',
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.75rem',
    outline: {
      default: 'none',
      ':focus-visible': `0.125rem solid ${colors.focusRing}`,
    },
    paddingBlock: '0.7rem',
    paddingInline: '0.85rem',
    alignItems: 'center',
    backgroundColor: { default: 'transparent', ':hover:not(:disabled)': colors.surfaceRaised },
    boxSizing: 'border-box',
    color: { default: colors.textPrimarySoft, ':hover:not(:disabled)': colors.textPrimary },
    cursor: { default: 'pointer', ':disabled': 'not-allowed' },
    display: 'flex',
    fontFamily: 'inherit',
    fontSize: typography.sizeSm,
    justifyContent: 'space-between',
    opacity: {
      default: 1,
      ':disabled': 0.5,
    },
    outlineOffset: {
      default: 0,
      ':focus-visible': '0.125rem',
    },
    textAlign: 'left',
    transform: {
      default: 'none',
      ':hover:not(:disabled)': 'translateY(-1px)',
    },
    transitionDuration: '160ms',
    transitionProperty: 'background-color, border-color, color, transform',
    minHeight: '3rem',
  },
  exampleIcon: {
    color: colors.success,
    flexShrink: 0,
  },
});

export interface ExamplesProps {
  examples: string[];
  onPick: (example: string) => void;
  disabled?: boolean;
}

export function Examples({ examples, onPick, disabled = false }: ExamplesProps) {
  return (
    <div {...stylex.props(styles.askExamples)} aria-label="Suggested questions">
      <p {...stylex.props(styles.askExamplesHeading)}>Try asking</p>
      <div {...stylex.props(styles.askExamplesList)}>
        {examples.map((example) => (
          <button
            key={example}
            type="button"
            {...stylex.props(styles.askExampleBtn)}
            onClick={() => onPick(example)}
            disabled={disabled}
          >
            <span>{example}</span>
            <ArrowUpRight size={15} aria-hidden="true" {...stylex.props(styles.exampleIcon)} />
          </button>
        ))}
      </div>
    </div>
  );
}
