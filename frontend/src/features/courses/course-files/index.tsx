import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { FolderOpen, Search } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { useCourseFiles } from './useCourseFiles';
import type { CourseDetailPayload, CourseFileNode } from '@/lib/types';
import { colors, layout } from '@/styles/tokens.stylex';
import { TreeNode } from './tree';
import { LazyExternalMaterials } from './LazyExternalMaterials';

const styles = stylex.create({
  courseFilesSection: {
    borderColor: colors.border,
    borderRadius: layout.radiusLarge,
    borderStyle: 'solid',
    borderWidth: 1,
    overflow: 'hidden',
    backgroundColor: colors.surface,
    paddingBottom: '1rem',
  },
  coursePanelHeader: {
    gap: '1rem',
    paddingBlock: '1rem',
    paddingInline: '1rem',
    alignItems: 'center',
    display: 'flex',
    justifyContent: 'space-between',
  },
  courseFilesHeader: {
    alignItems: 'center',
    display: 'flex',
  },
  courseFileSearch: {
    marginBlock: 0,
    marginInline: 0,
    display: 'block',
    position: 'relative',
    width: 'min(21rem, 55%)',
  },
  coursePanelEmpty: {
    marginBlock: 0,
    marginInline: 0,
    paddingBlock: '2rem',
    paddingInline: '2rem',
    color: colors.textSecondary,
    textAlign: 'center',
  },
  treeView: {
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    marginBlock: 0,
    marginInline: '1rem',
    paddingBlock: '0.5rem',
    paddingInline: '0.5rem',
    backgroundColor: colors.background,
    boxShadow: 'none',
  },
});

function matches(node: CourseFileNode, query: string): boolean {
  return [node.name, node.local_path].some((value) =>
    String(value || '')
      .toLowerCase()
      .includes(query),
  );
}

function filterTree(node: CourseFileNode, query: string): CourseFileNode | null {
  if (!query || matches(node, query)) return node;
  const files = (node.files || []).filter((file) => matches(file, query));
  const children = (node.children || [])
    .map((child) => filterTree(child, query))
    .filter((child): child is CourseFileNode => Boolean(child));
  return files.length || children.length ? { ...node, files, children } : null;
}

function CourseFilesHeader({ search, setSearch }: { search: string; setSearch: (value: string) => void }) {
  return (
    <header {...stylex.props(styles.coursePanelHeader, styles.courseFilesHeader)}>
      <div>
        <span>Library</span>
        <h2 id="course-files-title">
          <FolderOpen /> Course files
        </h2>
      </div>
      <label {...stylex.props(styles.courseFileSearch)}>
        <Search aria-hidden="true" />
        <Input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Find a file…"
          aria-label="Find a course file"
        />
      </label>
    </header>
  );
}

function OfficialFileTree({
  data,
  tree,
  query,
}: {
  data: CourseDetailPayload;
  tree: CourseFileNode | null;
  query: string;
}) {
  if (!tree)
    return (
      <p {...stylex.props(styles.coursePanelEmpty)}>
        {query ? (
          'No matching eClass files.'
        ) : (
          <>
            No synchronized eClass files yet. <a href="/settings">Run a course check in Settings</a>.
          </>
        )}
      </p>
    );
  return (
    <div {...stylex.props(styles.treeView)} id="tree-view-container">
      <TreeNode
        node={tree}
        courseId={data.course.id}
        webdavFolder={data.course.webdav_folder || ''}
        studyLevels={data.study_levels || {}}
        insights={data.file_insights || {}}
        filesWithVersions={new Set(data.files_with_versions || [])}
        showDeleted={(data.folders_with_deleted || []).length > 0}
        collapsedFolders={new Set(data.collapsed_folders || [])}
        expandedFolders={new Set(data.expanded_folders || [])}
        forceOpen={Boolean(query)}
      />
    </div>
  );
}

export default function CourseFiles({ data }: { data: CourseDetailPayload }) {
  const [search, setSearch] = React.useState('');
  const [request, setRequest] = React.useState(0);
  const { files, error, knowledgeError, knowledgeLoading } = useCourseFiles(data.course.id, request);
  const current = files ? { ...data, ...files } : data;
  const query = search.trim().toLowerCase();
  const tree = current.tree ? filterTree(current.tree, query) : null;
  return (
    <section {...stylex.props(styles.courseFilesSection)} aria-labelledby="course-files-title">
      <CourseFilesHeader search={search} setSearch={setSearch} />
      {!files && !error ? <p {...stylex.props(styles.coursePanelEmpty)}>Loading course files…</p> : null}
      {error ? (
        <div {...stylex.props(styles.coursePanelEmpty)} role="alert">
          <p>The course files could not load.</p>
          <Button variant="secondary" onClick={() => setRequest((value) => value + 1)}>
            Try again
          </Button>
        </div>
      ) : null}
      {files && knowledgeLoading && (
        <p role="status" {...stylex.props(styles.coursePanelEmpty)}>
          Loading file status…
        </p>
      )}
      {files && knowledgeError && (
        <p role="alert" {...stylex.props(styles.coursePanelEmpty)}>
          File status could not load. <Button onClick={() => setRequest((value) => value + 1)}>Try again</Button>
        </p>
      )}
      {files ? <OfficialFileTree data={current} tree={tree} query={query} /> : null}
      {files ? <LazyExternalMaterials key={current.course.id} courseId={current.course.id} query={query} /> : null}
    </section>
  );
}
