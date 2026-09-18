import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { Pencil, Trash2 } from 'lucide-react';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import type { Conversation } from '@/features/ask/types';
import { commonStyles } from '@/styles/common';
import { colors, layout, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  askHistoryLink: {
    gap: '0.25rem',
    paddingBlock: '0.68rem',
    paddingInline: '0.7rem',
    textDecoration: 'none',
    color: colors.textPrimary,
    display: 'grid',
    minWidth: 0,
  },
  linkTitle: {
    overflow: 'hidden',
    color: colors.textPrimary,
    fontSize: typography.sizeSm,
    fontWeight: 550,
    letterSpacing: '-0.01em',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  linkPreview: {
    overflow: 'hidden',
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  linkTime: {
    overflow: 'hidden',
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  askHistoryRename: {
    padding: '0.45rem',
    gap: '0.25rem',
    display: 'flex',
    width: '100%',
  },
  askHistoryActions: {
    padding: '0.15rem',
    borderColor: colors.border,
    borderRadius: '0.45rem',
    borderStyle: 'solid',
    borderWidth: 1,
    gap: '0.15rem',
    alignItems: 'center',
    backgroundColor: colors.surface,
    display: 'flex',
    opacity: 0,
    pointerEvents: 'none',
    position: 'absolute',
    transitionDuration: '150ms',
    transitionProperty: 'opacity',
    zIndex: 2,
    right: '0.35rem',
    top: '0.4rem',
  },
  askHistoryActionsVisible: {
    opacity: 1,
    pointerEvents: 'auto',
  },
  askHistoryAction: {
    padding: 0,
    borderRadius: layout.radiusSmall,
    borderWidth: 0,
    alignItems: 'center',
    backgroundColor: { default: 'transparent', ':hover': colors.surfaceHover },
    color: { default: colors.textSecondary, ':hover': colors.textPrimary },
    cursor: 'pointer',
    display: 'inline-flex',
    justifyContent: 'center',
    height: '1.75rem',
    width: '1.75rem',
  },
  askHistoryError: {
    margin: 0,
    paddingBlock: '0.25rem',
    paddingInline: '0.5rem',
    color: colors.danger,
    fontSize: typography.sizeXs,
  },
  askHistoryItem: {
    borderColor: 'transparent',
    borderRadius: '0.6rem',
    borderStyle: 'solid',
    borderWidth: 1,
    backgroundColor: {
      default: 'transparent',
      ':focus-within': colors.surfaceRaised,
      ':hover': colors.surfaceRaised,
    },
    position: 'relative',
    minWidth: 0,
  },
  askHistoryItemActive: {
    borderColor: colors.borderLight,
    backgroundColor: colors.surfaceHover,
  },
});

function relativeTime(value: string | undefined): string {
  const date = new Date(String(value || '').replace(' ', 'T') + 'Z');
  if (Number.isNaN(date.getTime())) return '';
  const minutes = Math.max(0, Math.round((Date.now() - date.getTime()) / 60000));
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes} min ago`;
  if (minutes < 1440) return `${Math.round(minutes / 60)} hr ago`;
  return `${Math.round(minutes / 1440)} days ago`;
}

interface ConversationItemBodyProps {
  item: Conversation;
  active: boolean;
  editing: boolean;
  title: string;
  error: string | null;
  displayTitle: string;
  preview: string;
  onSave: (event: React.FormEvent) => void;
  onChangeTitle: (value: string) => void;
  onCancel: () => void;
  onRename: () => void;
  onDelete: () => void;
  canSave: boolean;
}

function ConversationLink({
  item,
  displayTitle,
  preview,
  active,
}: {
  item: Conversation;
  displayTitle: string;
  preview: string;
  active: boolean;
}) {
  return (
    <a
      {...stylex.props(styles.askHistoryLink)}
      href={`/ask?c=${encodeURIComponent(String(item.id))}`}
      title={displayTitle}
      aria-current={active ? 'page' : undefined}
    >
      <strong {...stylex.props(styles.linkTitle)}>{displayTitle}</strong>
      {preview && <span {...stylex.props(styles.linkPreview)}>{preview}</span>}
      <time {...stylex.props(styles.linkTime)}>{relativeTime(item.updated_at)}</time>
    </a>
  );
}

function ConversationRenameForm({
  id,
  title,
  onChange,
  onSubmit,
  onCancel,
  canSave,
}: {
  id: string | number;
  title: string;
  onChange: (value: string) => void;
  onSubmit: (event: React.FormEvent) => void;
  onCancel: () => void;
  canSave: boolean;
}) {
  return (
    <form {...stylex.props(styles.askHistoryRename)} onSubmit={onSubmit}>
      <label {...stylex.props(commonStyles.srOnly)} htmlFor={`conversation-title-${id}`}>
        Conversation title
      </label>
      <input
        id={`conversation-title-${id}`}
        value={title}
        onChange={(event) => onChange(event.target.value)}
        autoFocus
      />
      <button type="submit" disabled={!canSave}>
        Save
      </button>
      <button type="button" onClick={onCancel}>
        Cancel
      </button>
    </form>
  );
}

function ConversationActions({
  displayTitle,
  onRename,
  onDelete,
  visible,
}: {
  displayTitle: string;
  onRename: () => void;
  onDelete: () => void;
  visible: boolean;
}) {
  return (
    <div {...stylex.props(styles.askHistoryActions, visible && styles.askHistoryActionsVisible)}>
      <button
        type="button"
        {...stylex.props(styles.askHistoryAction)}
        onClick={onRename}
        aria-label={`Rename conversation ${displayTitle}`}
        title="Rename conversation"
      >
        <Pencil aria-hidden="true" />
      </button>
      <button
        type="button"
        {...stylex.props(styles.askHistoryAction)}
        onClick={onDelete}
        aria-label={`Delete conversation ${displayTitle}`}
        title="Delete conversation"
      >
        <Trash2 aria-hidden="true" />
      </button>
    </div>
  );
}

async function renameConversation({
  event,
  item,
  title,
  setEditing,
  onChanged,
  setError,
}: {
  event: React.FormEvent;
  item: Conversation;
  title: string;
  setEditing: (value: boolean) => void;
  onChanged: () => void;
  setError: (value: string) => void;
}) {
  event.preventDefault();
  if (!title.trim()) return;
  const response = await fetch(`/api/ask/conversations/${item.id}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title }),
  });
  if (response.ok) {
    setEditing(false);
    onChanged();
  } else setError('Could not rename this conversation.');
}

async function deleteConversation({
  item,
  setConfirmOpen,
  onChanged,
  setError,
}: {
  item: Conversation;
  setConfirmOpen: (value: boolean) => void;
  onChanged: () => void;
  setError: (value: string) => void;
}) {
  setConfirmOpen(false);
  const response = await fetch(`/api/ask/conversations/${item.id}`, { method: 'DELETE' });
  if (response.ok) onChanged();
  else setError('Could not delete this conversation.');
}

function ConversationItemError({ error }: { error: string | null }) {
  if (!error) return null;
  return (
    <p {...stylex.props(styles.askHistoryError)} role="alert">
      {error}
    </p>
  );
}

function ConversationItemBody({
  item,
  active,
  editing,
  title,
  error,
  displayTitle,
  preview,
  canSave,
  onSave,
  onChangeTitle,
  onCancel,
  onRename,
  onDelete,
}: ConversationItemBodyProps) {
  const [hovered, setHovered] = React.useState(false);
  return (
    <div
      {...stylex.props(styles.askHistoryItem, active && styles.askHistoryItemActive)}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onFocus={() => setHovered(true)}
      onBlur={(e) => !e.currentTarget.contains(e.relatedTarget) && setHovered(false)}
    >
      {editing ? (
        <ConversationRenameForm
          id={item.id}
          title={title}
          onChange={onChangeTitle}
          onSubmit={onSave}
          onCancel={onCancel}
          canSave={canSave}
        />
      ) : (
        <ConversationLink item={item} displayTitle={displayTitle} preview={preview} active={active} />
      )}
      <ConversationActions displayTitle={displayTitle} onRename={onRename} onDelete={onDelete} visible={hovered} />
      <ConversationItemError error={error} />
    </div>
  );
}

interface ConversationItemProps {
  item: Conversation;
  active: boolean;
  onChanged: () => void;
}

export function ConversationItem({ item, active, onChanged }: ConversationItemProps) {
  const [editing, setEditing] = React.useState(false);
  const [title, setTitle] = React.useState(item.title || item.display_title || 'Conversation');
  const [confirmOpen, setConfirmOpen] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const displayTitle = item.title || item.display_title || 'Conversation';
  const canSave = Boolean(title.trim());
  const preview =
    item.last_message_excerpt && !displayTitle.includes(item.last_message_excerpt) ? item.last_message_excerpt : '';
  const saveTitle = (event: React.FormEvent) =>
    renameConversation({ event, item, title, setEditing, onChanged, setError });
  const remove = () => deleteConversation({ item, setConfirmOpen, onChanged, setError });
  const cancelRename = () => {
    setTitle(item.title || item.display_title || 'Conversation');
    setEditing(false);
    setError(null);
  };
  return (
    <>
      <ConversationItemBody
        item={item}
        active={active}
        editing={editing}
        title={title}
        error={error}
        displayTitle={displayTitle}
        preview={preview}
        canSave={canSave}
        onSave={saveTitle}
        onChangeTitle={setTitle}
        onCancel={cancelRename}
        onRename={() => setEditing(true)}
        onDelete={() => setConfirmOpen(true)}
      />
      <ConfirmDialog
        open={confirmOpen}
        title="Delete conversation?"
        description={`Delete “${displayTitle}”? This removes the saved question and answer.`}
        confirmLabel="Delete conversation"
        danger
        onCancel={() => setConfirmOpen(false)}
        onConfirm={remove}
      />
    </>
  );
}
