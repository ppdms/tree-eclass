import * as stylex from '@stylexjs/stylex';
import { z } from 'zod/v4';
import { media } from '@/styles/constants.stylex';
import { colors, typography } from '@/styles/tokens.stylex';

export const catalogStyles = stylex.create({
  settingsFormGrid: {
    gap: '1rem',
    display: 'grid',
  },
  settingsSectionDescription: {
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    marginBlock: '0.35rem',
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
  catalogList: {
    display: 'grid',
    gap: '0.75rem',
    listStyle: 'none',
    marginBlock: 0,
    marginInline: 0,
    paddingBlock: 0,
    paddingInline: 0,
  },
  catalogRow: {
    alignItems: 'start',
    borderColor: colors.border,
    borderRadius: '0.5rem',
    borderStyle: 'solid',
    borderWidth: 1,
    display: 'grid',
    gap: '0.75rem',
    gridTemplateColumns: {
      default: 'minmax(0, 1fr) auto',
      [media.mobile]: 'minmax(0, 1fr)',
    },
    paddingBlock: '0.75rem',
    paddingInline: '0.75rem',
  },
  catalogTitle: {
    fontSize: typography.sizeSm,
    fontWeight: 600,
    marginBlock: 0,
  },
  catalogMeta: {
    color: colors.textSecondary,
    display: 'block',
    fontSize: typography.sizeXs,
    marginTop: '0.2rem',
  },
  catalogFields: {
    display: 'grid',
    gap: '0.5rem',
    gridTemplateColumns: {
      default: 'repeat(2, minmax(0, 1fr))',
      [media.mobile]: 'minmax(0, 1fr)',
    },
    marginTop: '0.5rem',
  },
  catalogInput: {
    backgroundColor: colors.surface,
    borderColor: colors.border,
    borderRadius: '0.375rem',
    borderStyle: 'solid',
    borderWidth: 1,
    color: colors.textPrimary,
    fontSize: typography.sizeSm,
    paddingBlock: '0.4rem',
    paddingInline: '0.55rem',
    width: '100%',
  },
  catalogActions: {
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
    gap: '0.5rem',
  },
  catalogToolbar: {
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
    gap: '0.75rem',
  },
  catalogSearch: {
    backgroundColor: colors.surface,
    borderColor: colors.border,
    borderRadius: '0.375rem',
    borderStyle: 'solid',
    borderWidth: 1,
    color: colors.textPrimary,
    fontSize: typography.sizeSm,
    minWidth: '12rem',
    paddingBlock: '0.4rem',
    paddingInline: '0.55rem',
  },
  catalogEmpty: {
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
  },
  registeredBadge: {
    backgroundColor: colors.surfaceRaised,
    borderRadius: '999px',
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    paddingBlock: '0.15rem',
    paddingInline: '0.6rem',
  },
});

export const availableCourseSchema = z.object({
  id: z.union([z.number(), z.string()]),
  code: z.string().optional().default(''),
  title: z.string().optional().default(''),
  professor: z.string().optional().default(''),
  name: z.string().optional().default(''),
  registered: z.boolean().optional().default(false),
});

export const availablePayloadSchema = z.object({
  courses: z.array(availableCourseSchema).optional().default([]),
});

export interface AvailableCourse {
  id: string;
  code: string;
  title: string;
  professor: string;
  name: string;
  registered: boolean;
}

export interface CatalogRowState {
  name: string;
  shortName: string;
  busy: boolean;
  error: string | null;
}
