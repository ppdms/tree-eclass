import * as stylex from '@stylexjs/stylex';
import { PanelTop } from 'lucide-react';
import { colors } from '@/styles/tokens.stylex';
import type { StudyAction } from '@/lib/types';
import type { SessionInfo } from '@/features/session/reader/types';
import { formatMinutes } from './formatMinutes';
import { BookmarkAction, DrawerToggle, FinishMenu, SelectionNoteAction } from './SessionToolbarButtons';

const styles = stylex.create({
  actions: {
    gap: '0.375rem',
    alignItems: 'center',
    display: 'flex',
    flexShrink: 0,
    marginLeft: 'auto',
  },
  time: {
    borderColor: colors.border,
    borderRadius: '0.5rem',
    borderWidth: '1px',
    paddingInline: '0.5rem',
    alignItems: 'center',
    backgroundColor: colors.surface,
    color: colors.textSecondary,
    display: 'inline-flex',
    fontSize: '0.75rem',
    fontVariantNumeric: 'tabular-nums',
    marginRight: '0.5rem',
    minHeight: '2rem',
  },
  statusDot: {
    borderRadius: '999px',
    backgroundColor: colors.success,
    display: 'inline-block',
    flexShrink: 0,
    marginInlineEnd: 4,
    outlineColor: colors.surfaceSuccess,
    outlineStyle: 'solid',
    outlineWidth: 3,
    height: 6,
    width: 6,
  },
  statusDotIdle: {
    backgroundColor: colors.warning,
    outlineColor: colors.surfaceWarning,
  },
});

export interface ToolbarActionsProps {
  measuredSeconds: number;
  idle: boolean;
  annotationCount: number;
  drawerTab: string | null;
  onToggleDrawer: (tab: string) => void;
  onBookmark: (targetPage?: number) => void | Promise<void>;
  pageBookmarked: boolean;
  action: StudyAction | null;
  session: SessionInfo | null;
  closing: string | null;
  closed: SessionInfo | null;
  questionCount: number;
  onFinish: (outcome: string) => void;
  selectionAvailable: boolean;
  onAddNote: () => void;
}

export default function ToolbarActions({
  measuredSeconds,
  idle,
  annotationCount,
  drawerTab,
  onToggleDrawer,
  onBookmark,
  pageBookmarked,
  action,
  session,
  closing,
  closed,
  questionCount,
  onFinish,
  selectionAvailable,
  onAddNote,
}: ToolbarActionsProps) {
  return (
    <span {...stylex.props(styles.actions)}>
      <span {...stylex.props(styles.time)} aria-label="Session time">
        <span {...stylex.props(styles.statusDot, idle && styles.statusDotIdle)} aria-hidden="true" />
        {formatMinutes(measuredSeconds)}
        {idle ? ' (paused)' : ''}
      </span>
      <BookmarkAction bookmarked={pageBookmarked} onClick={() => onBookmark()} />
      <SelectionNoteAction available={selectionAvailable} onClick={onAddNote} />
      <DrawerToggle
        icon={PanelTop}
        label="Inspector"
        active={Boolean(drawerTab)}
        badge={annotationCount || null}
        onClick={() => onToggleDrawer(drawerTab || 'insight')}
      />
      <FinishMenu
        action={action}
        session={session}
        closing={closing}
        closed={closed}
        questionCount={questionCount}
        onFinish={onFinish}
      />
    </span>
  );
}
