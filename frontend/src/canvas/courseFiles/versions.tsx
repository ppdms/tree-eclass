import type { CourseVersion } from '@/lib/types';
import { AppLink } from '../app/links';
import { Button, Notice, Panel } from '../ui/controls';
import { Icon, Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { fileHref, fileIdentity, formatTimestamp } from './helpers';
import { useHistory } from './useHistory';

function versionStatus(version: CourseVersion, deleted: boolean, href: string | null, timestamp: string): string {
  return deleted
    ? href
      ? `(deleted ${timestamp})`
      : `(deleted ${timestamp}, no archived copy)`
    : version.redirect_url
      ? timestamp
      : href
        ? ''
        : `${timestamp} (no file)`;
}

function VersionEntry({ version, deleted, panelId }: { version: CourseVersion; deleted: boolean; panelId: string }) {
  const name = version.display_name || version.file_path.split('/').pop() || version.file_path;
  const timestamp = formatTimestamp(version.timestamp);
  const id = fileIdentity(
    'archive-entry',
    panelId,
    version.file_path,
    version.timestamp,
    version.redirect_url,
    version.version_webdav_path,
    version.diff_webdav_path,
    version.display_name,
  );
  const href = version.redirect_url || (version.version_webdav_path ? fileHref(version.version_webdav_path) : null);
  const label = deleted ? name : version.redirect_url ? 'Old external link' : `Version from ${timestamp}`;
  return (
    <View id={id} style={{ gap: 5 }}>
      <View style={{ flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 10 }}>
        {href ? (
          <AppLink id={`${id}-source`} href={href} target="_blank" rel="noopener noreferrer">
            {label}
          </AppLink>
        ) : deleted ? (
          <Text>{name}</Text>
        ) : null}
        <Text style={{ color: colors.muted, fontSize: 12 }}>{versionStatus(version, deleted, href, timestamp)}</Text>
        {!deleted && !version.redirect_url && version.version_webdav_path && version.diff_webdav_path ? (
          <AppLink
            id={`${id}-diff`}
            href={fileHref(version.diff_webdav_path)}
            target="_blank"
            rel="noopener noreferrer"
            label="Open visual diff"
            title="Open visual diff"
            block
          >
            <Icon name="file-diff" style={{ width: 18, height: 18, color: colors.link }} />
          </AppLink>
        ) : null}
      </View>
      {deleted && <Text style={{ fontSize: 12, color: colors.muted, mono: true }}>{version.file_path}</Text>}
    </View>
  );
}

function VersionPanel({
  id,
  entries,
  error,
  deleted,
}: {
  id: string;
  entries: CourseVersion[];
  error: string | null;
  deleted: boolean;
}) {
  if (error)
    return (
      <Notice id={id} tone="danger">
        <Text>{error}</Text>
      </Notice>
    );
  return (
    <Panel id={id} style={{ gap: 12, marginTop: 6, backgroundColor: colors.raised }}>
      <Text style={{ fontWeight: 650 }}>{deleted ? 'Deleted files:' : 'Old versions:'}</Text>
      {entries.length ? (
        entries.map((version) => (
          <VersionEntry
            key={fileIdentity(
              'version',
              version.file_path,
              version.timestamp,
              version.version_webdav_path,
              version.redirect_url,
              version.diff_webdav_path,
              version.display_name,
            )}
            panelId={id}
            version={version}
            deleted={deleted}
          />
        ))
      ) : (
        <Text style={{ color: colors.muted }}>
          {deleted ? 'No deleted files found.' : 'No archived versions found.'}
        </Text>
      )}
    </Panel>
  );
}

export function FileVersionsButton({
  courseId,
  filePath,
  rowId,
}: {
  courseId: string | number;
  filePath: string;
  rowId?: string;
}) {
  const id = fileIdentity('file-versions', courseId, filePath, rowId);
  const query = new URLSearchParams({ file_path: filePath });
  const url = `/api/courses/${encodeURIComponent(String(courseId))}/file-versions?${query}`;
  const { open, entries, error, pending, toggle } = useHistory(url, false);
  return (
    <View>
      <Button
        id={id}
        label="View versions for this file"
        title="View versions"
        expanded={open}
        controls={`${id}-panel`}
        disabled={pending}
        onClick={() => {
          void toggle();
        }}
      >
        Versions
      </Button>
      {open && <VersionPanel id={`${id}-panel`} entries={entries} error={error} deleted={false} />}
    </View>
  );
}

export function DeletedFilesButton({ courseId, folderKey }: { courseId: string | number; folderKey: string }) {
  const id = fileIdentity('deleted-files', courseId, folderKey);
  const query = new URLSearchParams({ folder: folderKey });
  const url = `/api/courses/${encodeURIComponent(String(courseId))}/deleted-files?${query}`;
  const { open, entries, error, pending, toggle } = useHistory(url, true);
  return (
    <View>
      <Button
        id={id}
        label="Show deleted files"
        title="Show deleted files"
        expanded={open}
        controls={`${id}-panel`}
        disabled={pending}
        onClick={() => {
          void toggle();
        }}
      >
        Deleted files
      </Button>
      {open && <VersionPanel id={`${id}-panel`} entries={entries} error={error} deleted />}
    </View>
  );
}
