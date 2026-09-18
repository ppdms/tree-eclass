import * as stylex from '@stylexjs/stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';

export const diffTreeStyles = stylex.create({
  diffTreeBranch: {
    color: colors.borderLight,
    flexShrink: 0,
    fontFamily: typography.fontMono,
    fontSize: typography.sizeBase,
    lineHeight: 1.017857,
    userSelect: 'none',
    whiteSpace: 'pre',
  },
  diffTreeIcon: {
    alignItems: 'center',
    display: 'inline-flex',
    flexShrink: 0,
    fontSize: typography.sizeBase,
    justifyContent: 'center',
    lineHeight: 1,
    textAlign: 'center',
    width: '1.25rem',
  },
  diffTreeSymbol: {
    alignItems: 'center',
    display: 'inline-flex',
    flexShrink: 0,
    fontFamily: typography.fontMono,
    fontSize: typography.sizeBase,
    fontWeight: typography.weightBold,
    justifyContent: 'center',
    lineHeight: 1,
    textAlign: 'center',
    width: '1rem',
  },
  diffTreeNode: {
    gap: '0.25rem',
    paddingBlock: '0.025rem',
    alignItems: 'center',
    display: 'flex',
  },
  diffTreeDirectory: {
    color: colors.textPrimary,
  },
  diffTreeFile: {
    color: colors.textPrimary,
  },
  addedFile: {
    color: colors.success,
  },
  addedDirectory: {
    color: colors.success,
  },
  deletedFile: {
    color: colors.danger,
    opacity: 0.75,
  },
  deletedDirectory: {
    color: colors.danger,
    opacity: 0.75,
  },
  modifiedFile: {
    color: colors.warning,
  },
  modifiedDirectory: {
    color: colors.warning,
  },
  mixed: {
    color: colors.infoStrong,
  },
  unchanged: {},
  diffTreeName: {
    alignItems: 'center',
    display: 'inline-flex',
    fontFamily: typography.fontMono,
    fontSize: typography.sizeSm,
    fontWeight: typography.weightNormal,
    justifyContent: 'center',
    lineHeight: 1,
    overflowWrap: 'anywhere',
    minWidth: 0,
  },
  diffTreeDirectoryName: {
    fontWeight: typography.weightSemibold,
  },
  diffTreeNameDeleted: {
    textDecoration: 'line-through',
  },
  diffTreeLink: {
    textDecoration: {
      default: 'none',
      ':hover': 'underline',
    },
    alignItems: 'center',
    color: 'inherit',
    display: 'inline-flex',
    lineHeight: 'inherit',
  },
  diffPdfBtn: {
    padding: 0,
    borderColor: {
      default: 'transparent',
      ':hover': colors.borderLight,
    },
    borderRadius: layout.radiusPill,
    borderStyle: 'solid',
    borderWidth: 1,
    textDecoration: 'none',
    alignItems: 'center',
    backgroundColor: {
      default: 'transparent',
      ':hover': colors.surfaceHover,
    },
    color: {
      default: colors.textSecondary,
      ':hover': colors.textPrimary,
    },
    cursor: 'pointer',
    display: 'inline-flex',
    fontSize: '0.875rem',
    justifyContent: 'center',
    lineHeight: 1,
    transitionDuration: '150ms',
    transitionProperty: 'color, background-color, border-color',
    verticalAlign: 'middle',
    height: '1.5rem',
    marginLeft: '0.25rem',
    minHeight: '1.5rem',
    minWidth: '1.5rem',
    width: '1.5rem',
  },
  diffTreeEmpty: {
    margin: 0,
    paddingBlock: '0.5rem',
    color: colors.textSecondary,
    fontSize: '0.8125rem',
  },
  diffTreeView: {
    paddingBlock: '0.35rem',
    paddingInline: '0.625rem',
    display: 'flex',
    flexDirection: 'column',
    fontFamily: typography.fontMono,
    fontSize: typography.sizeSm,
  },
  diffTreeLines: {},
});
