import * as stylex from '@stylexjs/stylex';
import { media } from '@/styles/constants.stylex';
import { colors, layout, typography } from '@/styles/tokens.stylex';

export const knowledgeStyles = stylex.create({
  totalTile: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.15rem',
    paddingBlock: '0.65rem',
    paddingInline: '0.75rem',
    backgroundColor: colors.surfaceRaised,
    display: 'grid',
  },
  tileValue: {
    fontSize: '1.1rem',
  },
  tileLabel: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  settingsKnowledgeSummary: {
    gap: '0.5rem',
    marginBlock: '0.75rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(4, minmax(0, 1fr))',
      [media.mobile]: 'repeat(2, minmax(0, 1fr))',
    },
  },
  courseCoverageGrid: {
    gap: '0.45rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.tablet]: 'minmax(0, 1fr)',
    },
  },
  settingsCourseIndex: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '0.55rem',
    paddingInline: '0.65rem',
    alignItems: 'center',
    backgroundColor: colors.surfaceRaised,
    color: colors.textPrimary,
    columnGap: '0.75rem',
    display: 'grid',
    gridTemplateColumns: 'minmax(0, 1fr) auto',
    rowGap: '0.35rem',
    minWidth: 0,
  },
  courseName: {
    overflow: 'hidden',
    fontSize: '0.75rem',
    fontWeight: typography.weightSemibold,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  percentText: {
    color: colors.success,
    fontSize: typography.sizeXs,
    fontVariantNumeric: 'tabular-nums',
  },
  progressBar: {
    borderRadius: '9999px',
    overflow: 'hidden',
    backgroundColor: colors.surfaceHover,
    display: 'block',
    gridColumnEnd: '-1',
    gridColumnStart: '1',
    height: '0.2rem',
  },
  settingsCourseIndexEmpty: {
    opacity: 0.6,
  },
  settingsCourseIndexFailed: {
    borderColor: colors.surfaceDanger,
    color: colors.danger,
  },
  settingsCourseIndexPending: {
    borderColor: colors.surfaceWarning,
    color: colors.warning,
  },
  settingsCourseIndexReady: {},
  progressFill: (width: string) => ({
    backgroundColor: colors.primary,
    display: 'block',
    height: '100%',
    width,
  }),
  progressFillSuccess: {
    backgroundColor: colors.success,
  },
  progressFillWarning: {
    backgroundColor: colors.warning,
  },
  progressFillDanger: {
    backgroundColor: colors.danger,
  },
  statusSmall: {
    color: colors.warning,
    fontSize: typography.sizeXs,
    gridColumnEnd: '-1',
    gridColumnStart: '1',
  },
  statusSmallDanger: {
    color: colors.danger,
    fontSize: typography.sizeXs,
    gridColumnEnd: '-1',
    gridColumnStart: '1',
  },
  settingsCourseCoverage: {
    marginBlock: '1rem',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
    paddingTop: '0.9rem',
  },
  coverageSummary: {
    gap: '.75rem',
    listStyle: 'none',
    alignItems: 'baseline',
    color: colors.textPrimary,
    cursor: 'pointer',
    display: 'flex',
    fontSize: '0.78rem',
    fontWeight: typography.weightSemibold,
    justifyContent: 'space-between',
  },
  coverageSummaryMeta: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontWeight: typography.weightNormal,
  },
  coverageBody: {
    marginTop: '1rem',
  },
  settingsMaintenanceActions: {
    gap: '0.5rem',
    display: 'flex',
    flexWrap: 'wrap',
    marginTop: '1rem',
  },

  settingsFormError: {
    color: colors.danger,
    fontSize: typography.sizeSm,
    marginTop: '0.75rem',
  },
  settingsFormSuccess: {
    color: colors.success,
    fontSize: typography.sizeSm,
    marginTop: '0.75rem',
  },
});
