import * as stylex from '@stylexjs/stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';

export const buttonStyles = stylex.create({
  base: {
    borderRadius: layout.radius,
    gap: '.4rem',
    outline: {
      default: 'none',
      ':focus-visible': `0.125rem solid ${colors.focusRing}`,
    },
    paddingBlock: '.5rem',
    paddingInline: '1rem',
    textDecoration: 'none',
    alignItems: 'center',
    boxSizing: 'border-box',
    cursor: { default: 'pointer', ':disabled': 'not-allowed' },
    display: 'inline-flex',
    flexShrink: 0,
    fontSize: typography.sizeBase,
    fontWeight: 550,
    justifyContent: 'center',
    lineHeight: 1.25,
    opacity: { default: 1, ':disabled': 0.6 },
    outlineOffset: {
      default: 0,
      ':focus-visible': '0.125rem',
    },
    textAlign: 'center',
    transitionDuration: '150ms',
    transitionProperty: 'background-color, border-color, color, transform',
    height: layout.controlHeight,
    minHeight: '2.75rem',
    minWidth: '2.75rem',
  },
  primary: {
    borderColor: 'transparent',
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: { default: colors.primary, ':hover': colors.primaryDim },
    color: { default: colors.textOnPrimary, ':hover': colors.textOnPrimary },
  },
  secondary: {
    borderColor: { default: colors.border, ':hover': colors.borderLight },
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: { default: colors.surfaceRaised, ':hover': colors.surfaceHover },
    color: { default: colors.textPrimary, ':hover': colors.textPrimary },
  },
  outline: {
    borderColor: colors.border,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: 'transparent',
    color: colors.textPrimary,
  },
});

export const styles = stylex.create({
  cardHeader: {
    padding: 16,
    gap: 4,
    display: 'flex',
    flexDirection: 'column',
  },
  cardTitle: {
    fontWeight: typography.weightSemibold,
    letterSpacing: '-0.01em',
    lineHeight: 1,
  },
  cardDescription: {
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
  },
  cardContent: {
    paddingInline: 16,
    paddingBlockEnd: 16,
  },
  cardFooter: {
    paddingInline: 16,
    alignItems: 'center',
    display: 'flex',
    paddingBlockEnd: 16,
  },
  field: {
    borderColor: colors.border,
    borderRadius: 6,
    borderStyle: 'solid',
    borderWidth: 1,
    outline: {
      default: 'none',
      ':focus-visible': `2px solid ${colors.focusRing}`,
      ':focus': 'none',
    },
    paddingBlock: 8,
    paddingInline: 12,
    backgroundColor: {
      default: colors.surface,
      ':focus': colors.surface,
    },
    color: { default: colors.textPrimary, '::placeholder': colors.textSecondary },
    cursor: { default: 'auto', ':disabled': 'not-allowed' },
    display: 'flex',
    fontFamily: 'inherit',
    fontSize: typography.sizeSm,
    inlineSize: '100%',
    lineHeight: 1.25,
    minBlockSize: 40,
    opacity: { default: 1, ':disabled': 0.5 },
    transitionProperty: 'background-color, border-color, color',
  },
  label: {
    color: colors.textPrimary,
    fontSize: typography.sizeSm,
    fontWeight: typography.weightMedium,
    lineHeight: 1.3,
  },
  hint: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    lineHeight: 1.4,
  },
  errorText: {
    color: colors.danger,
  },
  skeleton: {
    animationName: stylex.keyframes({
      from: { opacity: 0.55 },
      to: { opacity: 1 },
    }),
    animationDuration: '1.2s',
    animationIterationCount: 'infinite',
    animationTimingFunction: 'ease-in-out',
    backgroundColor: colors.surfaceRaised,
    borderRadius: 6,
  },
});
