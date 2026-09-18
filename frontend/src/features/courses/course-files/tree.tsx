import * as stylex from '@stylexjs/stylex';
import { Icon } from '@/components/Icon';
import * as React from 'react';
import type { CourseFileNode, FileInsight, StudyLevelValue } from '@/lib/types';
import { colors, layout, spacing, typography } from '@/styles/tokens.stylex';
import StudyLevel from '../../study/StudyLevel';
import { fileHref, formatTimestamp, relativePath } from './helpers';
import { FileVersionsButton, DeletedFilesButton } from './versions';
import { Insight } from './insight';

const styles = stylex.create({
  treeNode: {
    marginBlock: spacing.xs,
    marginInline: 0,
    lineHeight: 1.6,
  },
  treeNodeCollapsed: {
    opacity: 0.72,
  },
  treeFolderHeader: {
    gap: '.375rem',
    alignItems: 'center',
    display: 'flex',
    flexWrap: 'wrap',
  },
  folderToggle: {
    borderRadius: layout.radius,
    borderWidth: 0,
    outline: 'none',
    paddingBlock: '.375rem',
    paddingInline: '.375rem',
    alignItems: 'center',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceRaised },
    color: 'inherit',
    cursor: 'pointer',
    display: 'inline-flex',
    fontSize: '.875rem',
    textAlign: 'left',
    minHeight: '2rem',
    minWidth: 0,
  },
  folderChevron: {
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: typography.sizeSm,
    justifyContent: 'center',
    marginRight: '.375rem',
    width: '1rem',
  },
  treeIcon: {
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: '.9rem',
    marginRight: '.375rem',
  },
  treeName: {
    color: colors.textPrimary,
    fontWeight: 550,
  },
  treeChildren: {
    display: 'block',
    marginInlineStart: '1.125rem',
  },
  treeFile: {
    margin: 0,
    paddingBlock: '.15rem',
    paddingInline: 0,
    display: 'flex',
    flexDirection: 'column',
    lineHeight: 1.6,
  },
  treeFileRow: {
    borderRadius: layout.radiusSmall,
    gap: '.375rem',
    paddingBlock: '.15rem',
    paddingInline: '.35rem',
    alignItems: 'center',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceHover },
    display: 'flex',
    flexWrap: 'nowrap',
    minHeight: '2rem',
  },
  fileNameLink: {
    overflow: 'hidden',
    textDecoration: 'none',
    color: colors.textPrimary,
    fontFamily: 'Roboto Mono, Consolas, monospace',
    fontSize: typography.sizeBase,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    minWidth: 0,
  },
  fileSourceLink: {
    overflow: 'hidden',
    textDecoration: 'none',
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: '.78rem',
    justifyContent: 'center',
    opacity: 0.85,
    minHeight: '1.5rem',
    minWidth: '1.5rem',
  },
  fileSourceIcon: {
    verticalAlign: 'middle',
    height: '1rem',
    width: '1rem',
  },
  fileDate: {
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'inline-flex',
    flexBasis: 'auto',
    flexGrow: 0,
    flexShrink: 0,
    fontSize: typography.sizeXs,
    whiteSpace: 'nowrap',
    marginLeft: 'auto',
  },
});

export interface CourseTreeProps {
  node: CourseFileNode;
  courseId: string | number;
  webdavFolder: string;
  studyLevels: Record<string, StudyLevelValue>;
  insights: Record<string, FileInsight>;
  filesWithVersions: Set<string>;
  showDeleted: boolean;
  collapsedFolders: Set<string>;
  expandedFolders: Set<string>;
  forceOpen?: boolean;
  depth?: number;
}

function FileNameLink({ file, href }: { file: CourseFileNode; href: string }) {
  if (file.redirect_url)
    return (
      <a
        {...stylex.props(styles.fileNameLink)}
        href={file.redirect_url}
        target="_blank"
        rel="noopener noreferrer"
        aria-label={`Open ${file.name} in the external course source`}
      >
        {file.name}
      </a>
    );
  return (
    <a
      {...stylex.props(styles.fileNameLink)}
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      aria-label={`Open ${file.name}`}
    >
      {file.name}
    </a>
  );
}

interface FileRowProps {
  file: CourseFileNode;
  courseId: string | number;
  webdavFolder: string;
  studyLevels: Record<string, StudyLevelValue>;
  insights: Record<string, FileInsight>;
  filesWithVersions: Set<string>;
}

function FileRow({ file, courseId, webdavFolder, studyLevels, insights, filesWithVersions }: FileRowProps) {
  const path = file.local_path || file.url || file.name;
  const href = file.redirect_url || (file.local_path ? fileHref(file.local_path) : file.url) || '#';
  const relFile = relativePath(file.local_path, webdavFolder);
  const hasVersions = Boolean(relFile) && filesWithVersions.has(relFile);
  return (
    <div {...stylex.props(styles.treeFile)}>
      <div {...stylex.props(styles.treeFileRow)}>
        <StudyLevel courseId={courseId} filePath={path} initial={studyLevels[path] || 0} />
        <span {...stylex.props(styles.treeIcon)}>
          <Icon name={file.redirect_url ? 'box-arrow-up-right' : 'file-earmark'} aria-hidden="true" />
        </span>
        <FileNameLink file={file} href={href} />
        {file.url && (
          <a
            {...stylex.props(styles.fileSourceLink)}
            href={file.url}
            target="_blank"
            rel="noopener noreferrer"
            title="View original on eClass"
            aria-label={`Open original source for ${file.name} on eClass`}
          >
            <img src="/media/eclass-aueb.svg" alt="eClass" {...stylex.props(styles.fileSourceIcon)} />
          </a>
        )}
        {hasVersions && <FileVersionsButton courseId={courseId} filePath={relFile} />}
        {file.last_updated && <span {...stylex.props(styles.fileDate)}>{formatTimestamp(file.last_updated)}</span>}
      </div>
      <Insight insight={insights[file.local_path || '']} />
    </div>
  );
}

function FolderHeader({
  node,
  open,
  onToggle,
  depth,
  showDeleted,
  courseId,
}: {
  node: CourseFileNode;
  open: boolean;
  onToggle: () => void;
  depth: number;
  showDeleted: boolean;
  courseId: string | number;
}) {
  return (
    <div {...stylex.props(styles.treeFolderHeader)}>
      <button type="button" {...stylex.props(styles.folderToggle)} aria-expanded={open} onClick={onToggle}>
        <span {...stylex.props(styles.folderChevron)}>
          <Icon name={open ? 'chevron-down' : 'chevron-right'} aria-hidden="true" />
        </span>
        <span {...stylex.props(styles.treeIcon)}>
          <Icon name="folder-fill" aria-hidden="true" />
        </span>
        <span {...stylex.props(styles.treeName)}>{node.name}</span>
      </button>
      {depth === 0 && showDeleted && <DeletedFilesButton courseId={courseId} folderKey="" />}
    </div>
  );
}

interface TreeChildrenProps {
  files: CourseFileNode[];
  children: CourseFileNode[];
  courseId: string | number;
  webdavFolder: string;
  studyLevels: Record<string, StudyLevelValue>;
  insights: Record<string, FileInsight>;
  filesWithVersions: Set<string>;
  collapsedFolders: Set<string>;
  expandedFolders: Set<string>;
  forceOpen?: boolean;
  depth: number;
}

export function TreeChildren({
  files,
  children,
  courseId,
  webdavFolder,
  studyLevels,
  insights,
  filesWithVersions,
  collapsedFolders,
  expandedFolders,
  forceOpen,
  depth,
}: TreeChildrenProps) {
  return (
    <div {...stylex.props(styles.treeChildren)}>
      {files.map((file) => (
        <FileRow
          key={file.local_path || file.name}
          file={file}
          courseId={courseId}
          webdavFolder={webdavFolder}
          studyLevels={studyLevels}
          insights={insights}
          filesWithVersions={filesWithVersions}
        />
      ))}
      {children.map((child) => (
        <TreeNode
          key={child.url || child.name}
          node={child}
          courseId={courseId}
          webdavFolder={webdavFolder}
          studyLevels={studyLevels}
          insights={insights}
          filesWithVersions={filesWithVersions}
          showDeleted={false}
          collapsedFolders={collapsedFolders}
          expandedFolders={expandedFolders}
          forceOpen={forceOpen}
          depth={depth + 1}
        />
      ))}
    </div>
  );
}

async function persistFolderState(courseId: string | number, folderKey: string, collapsed: boolean): Promise<void> {
  const response = await fetch(`/api/courses/${courseId}/folders/collapsed`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({ folder_key: folderKey, collapsed }),
  });
  if (!response.ok) throw new Error(`Could not save folder state (${response.status})`);
}

function useFolderOpen(courseId: string | number, folderKey: string, initiallyOpen: boolean, forceOpen: boolean) {
  const [open, setOpen] = React.useState(initiallyOpen);
  const persistedOpen = React.useRef(initiallyOpen);
  const revision = React.useRef(0);
  const pending = React.useRef<Promise<void>>(Promise.resolve());
  const visible = forceOpen || open;
  const toggle = () => {
    if (forceOpen) return;
    const next = !open;
    const requestRevision = ++revision.current;
    setOpen(next);
    pending.current = pending.current.then(() =>
      persistFolderState(courseId, folderKey, !next).then(
        () => {
          persistedOpen.current = next;
        },
        () => {
          if (revision.current === requestRevision) setOpen(persistedOpen.current);
        },
      ),
    );
  };
  return { visible, toggle };
}

export function TreeNode({
  node,
  courseId,
  webdavFolder,
  studyLevels,
  insights,
  filesWithVersions,
  showDeleted,
  collapsedFolders,
  expandedFolders,
  forceOpen = false,
  depth = 0,
}: CourseTreeProps) {
  const folderKey = node.url || node.local_path || node.name;
  const files = node.files || [];
  const children = node.children || [];
  const initiallyOpen = expandedFolders.has(folderKey) || (depth === 0 && !collapsedFolders.has(folderKey));
  const { visible, toggle } = useFolderOpen(courseId, folderKey, initiallyOpen, forceOpen);
  if (!files.length && !children.length) return null;
  return (
    <div {...stylex.props(styles.treeNode, !visible && styles.treeNodeCollapsed)}>
      <FolderHeader
        node={node}
        open={visible}
        onToggle={toggle}
        depth={depth}
        showDeleted={showDeleted}
        courseId={courseId}
      />
      {visible && (
        <TreeChildren
          files={files}
          children={children}
          courseId={courseId}
          webdavFolder={webdavFolder}
          studyLevels={studyLevels}
          insights={insights}
          filesWithVersions={filesWithVersions}
          collapsedFolders={collapsedFolders}
          expandedFolders={expandedFolders}
          forceOpen={forceOpen}
          depth={depth}
        />
      )}
    </div>
  );
}
