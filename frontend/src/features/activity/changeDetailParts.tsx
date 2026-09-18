import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { lookup } from '@/lib/display';
import { media } from '@/styles/constants.stylex';
import { colors, effects, layout, typography } from '@/styles/tokens.stylex';
import { DiffTree } from './DiffTree';
import type { ChangeItem } from '@/lib/types';

const styles = stylex.create({
  changeDetailSummary: {
    gap: '1rem',
    display: 'flex',
    flexWrap: 'wrap',
    marginTop: '0.75rem',
  },
  changeSummaryStat: {
    gap: '0.35rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: typography.sizeSm,
    fontVariantNumeric: 'tabular-nums',
  },
  changeSummarySign: {
    fontFamily: typography.fontMono,
    fontWeight: typography.weightBold,
  },
  textSuccess: {
    color: colors.success,
  },
  textDestructive: {
    color: colors.danger,
  },
  textWarning: {
    color: colors.warning,
  },
  textMixed: {
    color: colors.infoStrong,
  },
  changeDetailItem: {
    gap: '1rem',
    paddingBlock: '0.65rem',
    alignItems: 'baseline',
    display: 'grid',
    gridTemplateColumns: {
      default: '7rem 1fr auto',
      [media.mobile]: '1fr',
    },
    borderBottomColor: colors.border,
    borderBottomStyle: 'solid',
    borderBottomWidth: 1,
  },
  changeDetailKind: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    fontWeight: typography.weightSemibold,
    letterSpacing: '0.04em',
    textTransform: 'uppercase',
  },
  changeDetailLabel: {
    color: colors.textPrimary,
    fontWeight: typography.weightSemibold,
  },
  changeDetailCode: {
    borderColor: colors.border,
    borderRadius: layout.radiusSmall,
    borderStyle: 'solid',
    borderWidth: 1,
    paddingBlock: '0.125rem',
    paddingInline: '0.25rem',
    backgroundColor: colors.surfaceHover,
    color: colors.textPrimary,
    fontFamily: typography.fontMono,
    fontSize: '0.78em',
    lineHeight: 1.35,
    overflowWrap: 'anywhere',
    marginLeft: '0.5rem',
  },
  changeDetailLinks: {
    gap: '0.75rem',
    alignItems: 'center',
    display: 'flex',
  },
  changeDetailLink: {
    gap: '0.25rem',
    textDecoration: {
      default: 'none',
      ':hover': 'underline',
    },
    alignItems: 'center',
    color: {
      default: colors.info,
      ':hover': colors.infoStrong,
    },
    display: 'inline-flex',
    fontSize: typography.sizeSm,
  },
  changeDetailHeader: {
    marginBottom: '1.75rem',
  },
  courseBreadcrumb: {
    gap: '0.35rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    flexWrap: 'wrap',
    fontSize: typography.sizeSm,
    overflowWrap: 'anywhere',
    marginBottom: '0.65rem',
    marginTop: 0,
  },
  breadcrumbLink: {
    textDecoration: {
      default: 'none',
      ':hover': 'underline',
    },
    color: {
      default: colors.textSecondary,
      ':hover': colors.textPrimary,
    },
  },
  breadcrumbSeparator: {
    color: colors.borderLight,
    userSelect: 'none',
  },
  headerTitle: {
    margin: 0,
    color: colors.textPrimary,
    fontSize: typography.size3xl,
    fontWeight: typography.weightBold,
    letterSpacing: typography.trackingTitle,
    lineHeight: 1.2,
    marginBottom: '0.5rem',
  },
  headerMessage: {
    margin: 0,
    color: colors.textPrimarySoft,
    fontSize: typography.sizeBase,
    lineHeight: typography.leadingRelaxed,
    marginBottom: '0.75rem',
  },
  changeDetailMeta: {
    gap: '1rem',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    flexWrap: 'wrap',
    fontSize: typography.sizeSm,
    marginTop: '0.5rem',
  },
  changeDetailCard: {
    padding: {
      default: '1.75rem',
      [media.mobile]: '1rem',
    },
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: colors.surface,
    boxShadow: effects.shadowSubtle,
    marginBottom: '1.5rem',
  },
  sectionTitle: {
    margin: 0,
    color: colors.textPrimary,
    fontSize: typography.sizeXl,
    fontWeight: typography.weightSemibold,
    letterSpacing: '-0.01em',
    lineHeight: 1.3,
    marginBottom: '1rem',
  },
  emptySectionText: {
    margin: 0,
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
  },
});

function fileHref(path: string | null | undefined): string | null {
  if (!path) return null;
  const normalized = String(path).startsWith('/') ? String(path) : `/${path}`;
  return `/files${encodeURI(normalized)}`;
}

const CHANGE_KIND_LABELS = {
  added_file: 'Added file',
  added_directory: 'Added folder',
  modified: 'Modified file',
  deleted: 'Deleted file',
  deleted_directory: 'Deleted folder',
} satisfies Record<string, string>;

function changeKindLabel(kind: string): string {
  return lookup(CHANGE_KIND_LABELS, kind, kind.replaceAll('_', ' '));
}

export function ChangeSummary({ changes = [] }: { changes?: ChangeItem[] }) {
  const counts = changes.reduce<Record<string, number>>((summary, item) => {
    const key = item.change_type || item.type || 'modified';
    summary[key] = (summary[key] || 0) + 1;
    return summary;
  }, {});
  return (
    <div {...stylex.props(styles.changeDetailSummary)} aria-label="Change summary">
      {Object.entries(counts).map(([kind, count]) => (
        <span {...stylex.props(styles.changeSummaryStat)} key={kind}>
          <span
            {...stylex.props(
              styles.changeSummarySign,
              kind.includes('added')
                ? styles.textSuccess
                : kind.includes('deleted')
                  ? styles.textDestructive
                  : kind === 'mixed'
                    ? styles.textMixed
                    : styles.textWarning,
            )}
          >
            {kind.includes('added') ? '+' : kind.includes('deleted') ? '−' : '~'}
          </span>
          {count} {changeKindLabel(kind)}
        </span>
      ))}
    </div>
  );
}

export function ChangeItem({ item }: { item: ChangeItem }) {
  const kind = item.change_type || item.type || 'modified';
  const kindLabel = changeKindLabel(kind);
  const path = item.file_path || item.path || item.name || 'Unnamed file';
  const label = item.display_name || path;
  const fileLink = item.redirect_url || fileHref(item.file_path);
  const diffLink = fileHref(item.diff_webdav_path);
  return (
    <li {...stylex.props(styles.changeDetailItem)}>
      <span {...stylex.props(styles.changeDetailKind)} aria-label={kindLabel}>
        {kindLabel}
      </span>
      <div>
        <strong {...stylex.props(styles.changeDetailLabel)}>{label}</strong>
        {label !== path && <code {...stylex.props(styles.changeDetailCode)}>{path}</code>}
      </div>
      <div {...stylex.props(styles.changeDetailLinks)}>
        {fileLink && (
          <a
            {...stylex.props(styles.changeDetailLink)}
            href={fileLink}
            target="_blank"
            rel="noopener"
            aria-label={`Open ${label}`}
          >
            Open file <span aria-hidden="true">↗</span>
          </a>
        )}
        {diffLink && (
          <a
            {...stylex.props(styles.changeDetailLink)}
            href={diffLink}
            target="_blank"
            rel="noopener"
            aria-label={`Open diff for ${label}`}
          >
            Open diff <span aria-hidden="true">↗</span>
          </a>
        )}
      </div>
    </li>
  );
}

export interface ChangeDetailHeaderProps {
  courseId: string;
  courseName: string;
  changeNo: string;
  record: { message?: string; timestamp?: string };
  changes: ChangeItem[];
  webdavFolder?: string;
}

export function ChangeDetailHeader({
  courseId,
  courseName,
  changeNo,
  record,
  changes,
  webdavFolder,
}: ChangeDetailHeaderProps) {
  return (
    <header {...stylex.props(styles.changeDetailHeader)}>
      <p {...stylex.props(styles.courseBreadcrumb)}>
        <a {...stylex.props(styles.breadcrumbLink)} href="/activity">
          Activity
        </a>{' '}
        <span {...stylex.props(styles.breadcrumbSeparator)} aria-hidden="true">
          /
        </span>{' '}
        <a {...stylex.props(styles.breadcrumbLink)} href={`/courses/${courseId}`}>
          {courseName}
        </a>{' '}
        <span {...stylex.props(styles.breadcrumbSeparator)} aria-hidden="true">
          /
        </span>{' '}
        Change {changeNo}
      </p>
      <h1 {...stylex.props(styles.headerTitle)}>Course files changed</h1>
      <p {...stylex.props(styles.headerMessage)}>{record.message || 'The course source tree was updated.'}</p>
      <div {...stylex.props(styles.changeDetailMeta)}>
        <time dateTime={record.timestamp || undefined}>{record.timestamp || 'Timestamp unavailable'}</time>
        <span>{webdavFolder || 'Course storage'}</span>
      </div>
      <ChangeSummary changes={changes} />
    </header>
  );
}

export function ChangedFilesSection({ changes, webdavFolder }: { changes: ChangeItem[]; webdavFolder?: string }) {
  return (
    <section {...stylex.props(styles.changeDetailCard)} aria-labelledby="change-files-title">
      <h2 id="change-files-title" {...stylex.props(styles.sectionTitle)}>
        Changed files
      </h2>
      {changes.length ? (
        <DiffTree changes={changes} webdavFolder={webdavFolder} />
      ) : (
        <p {...stylex.props(styles.emptySectionText)}>No file-level details were recorded for this change.</p>
      )}
    </section>
  );
}
