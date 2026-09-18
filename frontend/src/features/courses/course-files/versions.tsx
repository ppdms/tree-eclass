import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import { fetchJson } from '@/lib/api';
import type { CourseVersion } from '@/lib/types';
import { colors, layout, spacing, typography } from '@/styles/tokens.stylex';
import { fileHref, formatTimestamp } from './helpers';

const styles = stylex.create({
  versionEntry: {
    paddingBlock: '.375rem',
    lineHeight: 1.5,
    borderBottomColor: colors.borderLight,
    borderBottomStyle: 'solid',
    borderBottomWidth: 1,
  },
  versionTs: {
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    marginLeft: spacing.sm,
  },
  versionPath: {
    color: colors.textSecondary,
    fontFamily: 'Roboto Mono, monospace',
    fontSize: typography.sizeSm,
    opacity: 0.7,
    marginLeft: spacing.sm,
  },
  versionPanel: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    marginBlock: '.375rem',
    paddingBlock: spacing.sm,
    paddingInline: spacing.md,
    backgroundColor: colors.surfaceRaised,
    color: colors.textPrimary,
    display: 'block',
    fontSize: typography.sizeBase,
    marginLeft: '1.75rem',
  },
  versionPanelHeader: {
    color: colors.textSecondary,
    fontSize: typography.sizeSm,
    fontWeight: typography.weightSemibold,
    letterSpacing: '.04em',
    textTransform: 'uppercase',
    marginBottom: '.375rem',
  },
  versionError: {
    color: colors.danger,
  },
  versionEmpty: {
    color: colors.textSecondary,
  },
  versionIndicator: {
    padding: 0,
    borderRadius: layout.radiusSmall,
    borderStyle: 'none',
    alignItems: 'center',
    cursor: 'pointer',
    display: 'inline-flex',
    fontSize: typography.sizeBase,
    justifyContent: 'center',
    lineHeight: 1,
    opacity: 1,
    verticalAlign: 'middle',
    height: '1.375rem',
    marginLeft: spacing.sm,
    minHeight: '1.5rem',
    minWidth: '1.5rem',
    width: '1.375rem',
  },
  modifiedIndicator: {
    color: colors.info,
  },
  deletedIndicator: {
    borderColor: colors.danger,
    borderStyle: 'solid',
    borderWidth: 1,
    color: colors.danger,
    opacity: 0.95,
  },
  diffPdfBtn: {
    padding: 0,
    borderColor: 'transparent',
    borderRadius: '999px',
    borderStyle: 'solid',
    borderWidth: 1,
    textDecoration: 'none',
    alignItems: 'center',
    backgroundColor: 'transparent',
    color: colors.textSecondary,
    display: 'inline-flex',
    justifyContent: 'center',
    lineHeight: 1,
    verticalAlign: 'middle',
    height: '1.5rem',
    minHeight: '1.5rem',
    minWidth: '1.5rem',
    width: '1.5rem',
  },
});

interface VersionEntryProps {
  version: CourseVersion;
  deleted?: boolean;
}

function VersionDiff({ path }: { path: string }) {
  return (
    <span>
      {' '}
      <a
        href={fileHref(path)}
        target="_blank"
        rel="noopener noreferrer"
        {...stylex.props(styles.diffPdfBtn)}
        aria-label="Open visual diff"
        title="Open visual diff"
      >
        <Icon name="file-diff" aria-hidden="true" />
      </a>
    </span>
  );
}

function VersionPath({ path }: { path: string }) {
  return <span {...stylex.props(styles.versionPath)}>{path}</span>;
}

export function VersionEntry({ version, deleted = false }: VersionEntryProps) {
  const name = version.display_name || version.file_path.split('/').pop();
  const ts = formatTimestamp(version.timestamp);
  const diff = version.diff_webdav_path ? <VersionDiff path={version.diff_webdav_path} /> : null;
  if (version.redirect_url)
    return (
      <div {...stylex.props(styles.versionEntry)}>
        <Icon name="box-arrow-up-right" aria-hidden="true" />{' '}
        <a href={version.redirect_url} target="_blank" rel="noopener noreferrer">
          {deleted ? name : 'Old external link'}
        </a>{' '}
        <span {...stylex.props(styles.versionTs)}>{deleted ? `(deleted ${ts})` : ts}</span>
        {deleted && <VersionPath path={version.file_path} />}
      </div>
    );
  if (version.version_webdav_path)
    return (
      <div {...stylex.props(styles.versionEntry)}>
        <Icon name="file-earmark" aria-hidden="true" />{' '}
        <a href={fileHref(version.version_webdav_path)} target="_blank" rel="noopener noreferrer">
          {deleted ? name : `Version from ${ts}`}
        </a>
        {!deleted && diff}
        <span {...stylex.props(styles.versionTs)}>{deleted ? `(deleted ${ts})` : ''}</span>
        {deleted && <VersionPath path={version.file_path} />}
      </div>
    );
  return (
    <div {...stylex.props(styles.versionEntry)}>
      <span {...stylex.props(styles.versionTs)}>
        {deleted ? `(deleted ${ts}, no archived copy)` : `${ts} (no file)`}
      </span>
      {deleted && <VersionPath path={version.file_path} />}
    </div>
  );
}

export function VersionPanel({
  error,
  versions,
  deleted = false,
  header,
  emptyText,
}: {
  error: string | null;
  versions: CourseVersion[];
  deleted?: boolean;
  header: string;
  emptyText: string;
}) {
  return (
    <div {...stylex.props(styles.versionPanel)}>
      {error ? (
        <em {...stylex.props(styles.versionError)}>{error}</em>
      ) : (
        <>
          <div {...stylex.props(styles.versionPanelHeader)}>{header}</div>
          {versions.length ? (
            versions.map((version, index) => <VersionEntry version={version} deleted={deleted} key={index} />)
          ) : (
            <em {...stylex.props(styles.versionEmpty)}>{emptyText}</em>
          )}
        </>
      )}
    </div>
  );
}

export function FileVersionsButton({ courseId, filePath }: { courseId: string | number; filePath: string }) {
  const [open, setOpen] = React.useState(false);
  const [versions, setVersions] = React.useState<CourseVersion[] | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const toggle = async () => {
    if (open) {
      setOpen(false);
      return;
    }
    setError(null);
    try {
      const data = await fetchJson<{ versions?: CourseVersion[] }>(
        `api/courses/${courseId}/file-versions?file_path=${encodeURIComponent(filePath)}`,
      );
      setVersions(data.versions || []);
      setOpen(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load versions');
      setOpen(true);
    }
  };
  return (
    <>
      <button
        type="button"
        {...stylex.props(styles.versionIndicator, styles.modifiedIndicator)}
        aria-label="View versions for this file"
        title="View versions"
        onClick={toggle}
      >
        <Icon name="clock-history" aria-hidden="true" />
      </button>
      {open && (
        <VersionPanel
          error={error}
          versions={versions || []}
          header="Old versions:"
          emptyText="No archived versions found."
        />
      )}
    </>
  );
}

export function DeletedFilesButton({ courseId, folderKey }: { courseId: string | number; folderKey: string }) {
  const [open, setOpen] = React.useState(false);
  const [deleted, setDeleted] = React.useState<CourseVersion[] | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const toggle = async () => {
    if (open) {
      setOpen(false);
      return;
    }
    setError(null);
    try {
      const data = await fetchJson<{ deleted?: CourseVersion[] }>(
        `api/courses/${courseId}/deleted-files?folder=${encodeURIComponent(folderKey || '')}`,
      );
      setDeleted(data.deleted || []);
      setOpen(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load deleted files');
      setOpen(true);
    }
  };
  return (
    <>
      <button
        type="button"
        {...stylex.props(styles.versionIndicator, styles.deletedIndicator)}
        aria-label="Show deleted files"
        title="Show deleted files"
        onClick={toggle}
      >
        <Icon name="trash3" aria-hidden="true" />
      </button>
      {open && (
        <VersionPanel
          error={error}
          versions={deleted || []}
          deleted
          header="Deleted files:"
          emptyText="No deleted files found."
        />
      )}
    </>
  );
}
