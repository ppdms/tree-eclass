import * as stylex from '@stylexjs/stylex';
import { media } from '@/styles/constants.stylex';
import { colors, layout, spacing, typography } from '@/styles/tokens.stylex';

export const plannerStyles = stylex.create({
  plannerMessage: {
    padding: '0.75rem',
    borderRadius: layout.radiusMedium,
    borderStyle: 'solid',
    borderWidth: 1,
    fontSize: typography.sizeSm,
    marginBottom: '0.75rem',
  },
  plannerMessageWarn: {
    borderColor: colors.danger,
    backgroundColor: colors.surfaceDanger,
  },
  plannerMessageOk: {
    borderColor: colors.success,
    backgroundColor: colors.surfaceSuccess,
  },
  studyPlanWeekGrid: {
    gap: '0.45rem',
    display: 'grid',
    gridTemplateColumns: {
      default: 'repeat(7, 1fr)',
      [media.narrow]: 'repeat(2, 1fr)',
    },
  },
  weekGridLabel: {
    gap: '0.3rem',
    color: colors.textSecondary,
    display: 'grid',
    fontSize: typography.sizeXs,
    position: 'relative',
    textAlign: 'center',
  },
  weekGridUnit: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    pointerEvents: 'none',
    position: 'absolute',
    bottom: '0.65rem',
    right: '0.45rem',
  },
  weekGridInput: {
    textAlign: 'center',
    paddingRight: '1.8rem',
  },
  studyPlanControlGrid: {
    gap: '0.65rem',
    display: 'grid',
    gridTemplateColumns: {
      default: '1fr 1fr',
      [media.narrow]: '1fr',
    },
    marginTop: spacing.md,
  },
  controlGridLabel: {
    gap: '0.35rem',
    color: colors.textSecondary,
    display: 'grid',
    fontSize: typography.sizeXs,
  },
  studyPlanBlackout: {
    gridColumn: {
      default: '1 / -1',
      [media.narrow]: 'auto',
    },
  },
  studyPlanCapacity: {
    margin: 0,
    padding: '1rem',
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
  },
  studyPlanLegend: {
    paddingInline: '0.35rem',
    color: colors.textPrimary,
    fontWeight: 650,
  },
  studyPlanCourseList: {
    gap: '0.65rem',
    display: 'grid',
  },
  studyPlannerForm: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    minHeight: 0,
  },
  studyPlanSheetBody: {
    padding: {
      default: '1.25rem',
      [media.narrow]: '0.85rem',
    },
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    minHeight: 0,
    overflowY: 'auto',
  },
  studyPlanSheetFooter: {
    paddingBlock: '0.85rem',
    paddingInline: '1.25rem',
    backgroundColor: colors.surface,
    display: 'flex',
    justifyContent: 'flex-end',
    borderTopColor: colors.border,
    borderTopStyle: 'solid',
    borderTopWidth: 1,
  },
  studyPlanTabs: {
    marginBottom: '0.75rem',
    width: '100%',
  },
});
