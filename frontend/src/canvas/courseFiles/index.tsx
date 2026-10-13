import * as React from 'react';
import type { CourseDetailPayload, CourseFilesPayload, CourseFileNode } from '@/lib/types';
import { AppLink } from '../app/links';
import { ExternalMaterials } from '../materials';
import { Button, Notice, Panel } from '../ui/controls';
import { Field } from '../ui/forms';
import { Icon, Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { useFolderStates, type FolderStates } from './folders';
import { fileIdentity, filterTree } from './helpers';
import { TreeNode, type CourseTreeContext } from './tree';
import { useCourseFiles } from './useCourseFiles';

function FilesHeader({ id, search, setSearch }: { id: string; search: string; setSearch: (value: string) => void }) {
  return (
    <View style={{ flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'space-between', gap: 16 }}>
      <View style={{ gap: 5 }}>
        <Text style={{ color: colors.muted, fontSize: 12 }}>Library</Text>
        <View style={{ flexDirection: 'row', alignItems: 'center', gap: 8 }}>
          <Icon name="folder-open" style={{ width: 22, height: 22, color: colors.accent }} />
          <Text id={`${id}-title`} style={{ fontSize: 21, fontWeight: 650 }}>
            Course files
          </Text>
        </View>
      </View>
      <View style={{ width: 336, maxWidth: '100%', minWidth: 0 }}>
        <Field
          id={`${id}-search`}
          label="Find a course file"
          type="search"
          value={search}
          onChange={setSearch}
          placeholder="Find a file…"
        />
      </View>
    </View>
  );
}

function FilesStatus({
  id,
  loaded,
  error,
  metadataPending,
  metadataError,
  retry,
}: {
  id: string;
  loaded: boolean;
  error: boolean;
  metadataPending: boolean;
  metadataError: boolean;
  retry: () => void;
}) {
  return (
    <View style={{ gap: 10 }}>
      {!loaded && !error && <Text>Loading course files…</Text>}
      {error && (
        <Notice tone="danger">
          <Text>The course files could not load.</Text>
          <Button id={`${id}-retry-files`} onClick={retry}>
            Try again
          </Button>
        </Notice>
      )}
      {loaded && metadataPending && <Text style={{ color: colors.muted }}>Loading file status…</Text>}
      {loaded && metadataError && (
        <Notice tone="danger">
          <Text>File status could not load.</Text>
          <Button id={`${id}-retry-metadata`} onClick={retry}>
            Try again
          </Button>
        </Notice>
      )}
    </View>
  );
}

function useTreeContext(data: CourseDetailPayload, files: CourseFilesPayload | null, folders: FolderStates) {
  const versions = React.useMemo(() => new Set(files?.files_with_versions || []), [files?.files_with_versions]);
  const collapsed = React.useMemo(() => new Set(files?.collapsed_folders || []), [files?.collapsed_folders]);
  const expanded = React.useMemo(() => new Set(files?.expanded_folders || []), [files?.expanded_folders]);
  return React.useMemo<CourseTreeContext>(
    () => ({
      courseId: data.course.id,
      webdavFolder: data.course.webdav_folder || files?.tree?.local_path || '',
      studyLevels: files?.study_levels || {},
      insights: files?.file_insights || {},
      filesWithVersions: versions,
      collapsedFolders: collapsed,
      expandedFolders: expanded,
      showDeleted: Boolean(files?.folders_with_deleted?.length),
      folders,
    }),
    [data.course.id, data.course.webdav_folder, files, folders, versions, collapsed, expanded],
  );
}

function OfficialFileTree({
  tree,
  context,
  query,
  id,
}: {
  tree: CourseFileNode | null;
  context: CourseTreeContext;
  query: string;
  id: string;
}) {
  if (!tree) {
    return (
      <View style={{ padding: 24, gap: 8 }}>
        <Text style={{ color: colors.muted }}>
          {query ? 'No matching eClass files.' : 'No synchronized eClass files yet.'}
        </Text>
        {!query && (
          <AppLink id={`${id}-settings`} href="/settings" style={{ color: colors.link }}>
            Run a course check in Settings
          </AppLink>
        )}
      </View>
    );
  }
  return (
    <Panel id={`${id}-tree`} style={{ backgroundColor: colors.page, padding: 8 }}>
      <TreeNode node={tree} context={context} forceOpen={Boolean(query)} />
    </Panel>
  );
}

export function CourseFiles({ data }: { data: CourseDetailPayload }) {
  const [search, setSearch] = React.useState('');
  const [attempt, retry] = React.useReducer((value: number) => value + 1, 0);
  const { files, error, knowledgeError, knowledgeLoading, refreshMetadata } = useCourseFiles(data.course.id, attempt);
  const folders = useFolderStates(data.course.id);
  const context = useTreeContext(data, files, folders);
  const query = search.trim().toLowerCase();
  const tree = React.useMemo(() => (files?.tree ? filterTree(files.tree, query) : null), [files?.tree, query]);
  const id = fileIdentity('course-files', data.course.id);
  return (
    <View id={id} ready={Boolean(files || error) && !knowledgeLoading} style={{ gap: 18 }}>
      <Panel style={{ gap: 16 }}>
        <FilesHeader id={id} search={search} setSearch={setSearch} />
        <FilesStatus
          id={id}
          loaded={Boolean(files)}
          error={error}
          retry={retry}
          metadataPending={knowledgeLoading}
          metadataError={knowledgeError}
        />
        {files && <OfficialFileTree tree={tree} context={context} query={query} id={id} />}
      </Panel>
      {files && <ExternalMaterials courseId={data.course.id} search={query} onPublished={refreshMetadata} />}
    </View>
  );
}
