import type { CourseFileNode, FileInsight, StudyLevelValue } from '@/lib/types';
import { AppLink } from '../app/links';
import { Button } from '../ui/controls';
import { SourceImage } from '../ui/images';
import { Icon, Text, Vector, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { useFolderOpen, type FolderStates } from './folders';
import { fileIdentity, filePath, fileTarget, formatTimestamp, relativePath } from './helpers';
import { Insight } from './insight';
import { StudyLevel } from './level';
import { DeletedFilesButton, FileVersionsButton } from './versions';

export interface CourseTreeContext {
  courseId: string | number;
  webdavFolder: string;
  studyLevels: Record<string, StudyLevelValue>;
  insights: Record<string, FileInsight>;
  filesWithVersions: Set<string>;
  collapsedFolders: Set<string>;
  expandedFolders: Set<string>;
  folders: FolderStates;
  showDeleted: boolean;
}
interface TreeProps {
  node: CourseFileNode;
  context: CourseTreeContext;
  forceOpen?: boolean;
  depth?: number;
  ancestors?: string[];
}

function FileName({ file, id }: { file: CourseFileNode; id: string }) {
  return (
    <AppLink
      id={`${id}-open`}
      href={fileTarget(file)}
      target="_blank"
      rel="noopener noreferrer"
      title={file.name}
      label={file.redirect_url ? `Open ${file.name} in the external course source` : `Open ${file.name}`}
      style={{ mono: true, color: colors.text, minWidth: 0, maxLines: 1, flexShrink: 1 }}
    >
      {file.name}
    </AppLink>
  );
}

function FileSource({ file, id }: { file: CourseFileNode; id: string }) {
  if (!file.url) return null;
  return (
    <AppLink
      id={`${id}-original`}
      href={file.url}
      target="_blank"
      rel="noopener noreferrer"
      block
      title="View original on eClass"
      label={`Open original source for ${file.name} on eClass`}
      style={{ width: 24, height: 24, flexShrink: 0 }}
    >
      <SourceImage id={`${id}-eclass-image`} src="/media/eclass-aueb.svg" style={{ width: 24, height: 24 }} />
    </AppLink>
  );
}

function FileControls({ file, context, id }: { file: CourseFileNode; context: CourseTreeContext; id: string }) {
  const path = filePath(file);
  const relative = relativePath(file.local_path, context.webdavFolder);
  return (
    <View style={{ flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 8 }}>
      <StudyLevel
        id={`${id}-level`}
        courseId={context.courseId}
        filePath={path}
        initial={context.studyLevels[path] || 0}
      />
      <FileSource file={file} id={id} />
      {relative && context.filesWithVersions.has(relative) ? (
        <FileVersionsButton courseId={context.courseId} filePath={relative} rowId={id} />
      ) : null}
      {file.last_updated && (
        <Text style={{ color: colors.muted, fontSize: 11 }}>{formatTimestamp(file.last_updated)}</Text>
      )}
    </View>
  );
}

function FileRow({ file, context, parentId }: { file: CourseFileNode; context: CourseTreeContext; parentId: string }) {
  const id = fileIdentity('official-file', context.courseId, parentId, file.local_path, file.url, file.name);
  return (
    <View id={id} style={{ padding: 10, gap: 6, borderBottomWidth: 1, borderColor: colors.border, minWidth: 0 }}>
      <View style={{ flexDirection: 'row', alignItems: 'center', gap: 8, minWidth: 0 }}>
        {file.redirect_url ? (
          <Vector
            path="M2 9V14H14V9M7 2H14V9M14 2L6 10"
            stroke={colors.muted}
            style={{ width: 16, height: 16, flexShrink: 0 }}
          />
        ) : (
          <Icon name="file" style={{ width: 18, height: 18, color: colors.muted, flexShrink: 0 }} />
        )}
        <FileName file={file} id={id} />
      </View>
      <FileControls file={file} context={context} id={id} />
      <Insight insight={context.insights[file.local_path || '']} courseId={context.courseId} rowId={id} />
    </View>
  );
}

function FolderHeader({
  node,
  context,
  id,
  depth,
  open,
  toggle,
}: {
  node: CourseFileNode;
  context: CourseTreeContext;
  id: string;
  depth: number;
  open: boolean;
  toggle: () => void;
}) {
  return (
    <View style={{ flexDirection: 'row', flexWrap: 'wrap', alignItems: 'start', gap: 8 }}>
      <Button id={id} expanded={open} controls={`${id}-children`} onClick={toggle}>
        <Icon name={open ? 'folder-open' : 'folder'} style={{ width: 18, height: 18, color: colors.accent }} />
        <Text style={{ fontWeight: 600 }}>{node.name}</Text>
        <Icon name={open ? 'chevron-up' : 'chevron-down'} style={{ width: 12, height: 12 }} />
      </Button>
      {depth === 0 && context.showDeleted && <DeletedFilesButton courseId={context.courseId} folderKey="" />}
    </View>
  );
}

export function TreeNode({ node, context, forceOpen = false, depth = 0, ancestors = [] }: TreeProps) {
  const folderKey = node.url || node.local_path || node.name;
  const ancestry = [...ancestors, folderKey];
  const id = fileIdentity('official-folder', context.courseId, ancestry, node.local_path, node.name);
  const parentId = JSON.stringify(ancestry);
  const files = node.files || [];
  const children = node.children || [];
  const initiallyOpen =
    context.expandedFolders.has(folderKey) || (depth === 0 && !context.collapsedFolders.has(folderKey));
  const { visible, toggle } = useFolderOpen(context.courseId, folderKey, initiallyOpen, forceOpen, context.folders);
  if (!files.length && !children.length) return null;
  return (
    <View style={{ gap: 6, minWidth: 0 }}>
      <FolderHeader node={node} context={context} id={id} depth={depth} open={visible} toggle={toggle} />
      {visible && (
        <View id={`${id}-children`} style={{ gap: 6, paddingLeft: depth === 0 ? 0 : 14, minWidth: 0 }}>
          {files.map((file) => (
            <FileRow
              key={fileIdentity('file', file.local_path, file.url, file.name)}
              file={file}
              context={context}
              parentId={parentId}
            />
          ))}
          {children.map((child) => (
            <TreeNode
              key={fileIdentity('folder', child.url, child.local_path, child.name)}
              node={child}
              context={context}
              forceOpen={forceOpen}
              depth={depth + 1}
              ancestors={ancestry}
            />
          ))}
        </View>
      )}
    </View>
  );
}
