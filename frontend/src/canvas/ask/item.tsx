import { useState } from 'react';
import { AppLink } from '../app/links';
import { Button, Notice } from '../ui/controls';
import { Field } from '../ui/forms';
import { Dialog } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { conversationPreview, displayTitle, relativeTime, type Conversation } from './model';
import { useConversationItem, type ConversationItemState } from './itemState';
import { styles } from './styles';

function RenameEditor({ id, state }: { id: string; state: ConversationItemState }) {
  const save = () => {
    void state.mutate('rename');
  };
  return (
    <View style={styles.column}>
      <Field
        id={`${id}-title`}
        label="Conversation title"
        value={state.title}
        onChange={state.setTitle}
        disabled={Boolean(state.pending)}
        onSubmit={save}
      />
      <View style={styles.row}>
        <Button id={`${id}-save`} disabled={!state.title.trim() || Boolean(state.pending)} onClick={save}>
          {state.pending === 'rename' ? 'Saving…' : 'Save'}
        </Button>
        <Button id={`${id}-cancel-rename`} disabled={Boolean(state.pending)} onClick={state.cancel}>
          Cancel
        </Button>
      </View>
    </View>
  );
}

function DeleteDialog({ id, title, state }: { id: string; title: string; state: ConversationItemState }) {
  return (
    <Dialog
      id={`${id}-delete-dialog`}
      title="Delete conversation?"
      open={state.confirmOpen}
      onClose={() => state.setConfirmOpen(false)}
      actions={
        <View style={styles.row}>
          <Button id={`${id}-delete-cancel`} onClick={() => state.setConfirmOpen(false)}>
            Cancel
          </Button>
          <Button
            id={`${id}-delete-confirm`}
            tone="danger"
            disabled={Boolean(state.pending)}
            onClick={() => {
              void state.mutate('delete');
            }}
          >
            Delete conversation
          </Button>
        </View>
      }
    >
      <Text style={styles.text}>Delete “{title}”? This removes the saved question and answer.</Text>
    </Dialog>
  );
}

function ConversationLink({ item, active }: { item: Conversation; active: boolean }) {
  const title = displayTitle(item);
  const preview = conversationPreview(item);
  return (
    <AppLink
      id={`ask-conversation-${item.id}-open`}
      block
      href={`/ask?c=${encodeURIComponent(String(item.id))}`}
      style={styles.column}
      title={title}
      label={`${active ? 'Current conversation: ' : ''}${title}`}
    >
      <Text style={{ ...styles.text, fontWeight: 600, maxLines: 1 }}>{title}</Text>
      {preview && <Text style={{ ...styles.small, maxLines: 1 }}>{preview}</Text>}
      <Text style={styles.small}>{relativeTime(item.updated_at)}</Text>
    </AppLink>
  );
}

function ConversationActions({
  id,
  title,
  revealed,
  state,
}: {
  id: string;
  title: string;
  revealed: boolean;
  state: ConversationItemState;
}) {
  return (
    <View style={{ ...styles.row, position: 'absolute', top: 6, right: 6, display: revealed ? 'flex' : 'none' }}>
      <Button
        id={`${id}-rename`}
        label={`Rename conversation ${title}`}
        disabled={!revealed || Boolean(state.pending)}
        onClick={state.rename}
      >
        Rename
      </Button>
      <Button
        id={`${id}-delete`}
        label={`Delete conversation ${title}`}
        disabled={!revealed || Boolean(state.pending)}
        onClick={() => state.setConfirmOpen(true)}
      >
        {state.pending === 'delete' ? 'Deleting…' : 'Delete'}
      </Button>
    </View>
  );
}

export function ConversationItem({
  item,
  active,
  onChanged,
}: {
  item: Conversation;
  active: boolean;
  onChanged: () => void;
}) {
  const state = useConversationItem(item, onChanged);
  const [hovered, setHovered] = useState(false);
  const [focused, setFocused] = useState(false);
  const revealed = hovered || focused || state.confirmOpen || Boolean(state.pending);
  const title = displayTitle(item);
  const id = `ask-conversation-${item.id}`;
  const itemStyle = active ? { ...styles.historyItem, ...styles.activeItem } : styles.historyItem;
  return (
    <View
      id={id}
      style={itemStyle}
      onHoverChange={setHovered}
      onFocusWithinChange={(value) => {
        if (!state.confirmOpen || value) setFocused(value);
      }}
    >
      {state.editing ? <RenameEditor id={id} state={state} /> : <ConversationLink item={item} active={active} />}
      {!state.editing && <ConversationActions id={id} title={title} revealed={revealed} state={state} />}
      {state.error && (
        <Notice id={`${id}-error`} tone="danger">
          <Text style={styles.small}>{state.error}</Text>
        </Notice>
      )}
      <DeleteDialog id={id} title={title} state={state} />
    </View>
  );
}
