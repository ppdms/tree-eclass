import { Button, Notice } from '../ui/controls';
import { Field } from '../ui/forms';
import { Popover } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';
import type { MarkCreate, Selection } from './types';

export interface SelectionEditorProps {
  selection: Selection | null;
  note: boolean;
  draft: string;
  busy: boolean;
  error: string | null;
  onDraft: (value: string) => void;
  onNote: (value: boolean) => void;
  onDismiss: () => void;
  onSave: (kind: MarkCreate['kind'], color: string) => void;
}
export function SelectionEditor(props: SelectionEditorProps) {
  const selection = props.selection;
  if (!selection) return null;
  const id = `selection:${encodeURIComponent(
    JSON.stringify([
      selection.documentId,
      selection.context.source_hash,
      selection.page,
      selection.char_start,
      selection.char_end,
    ]),
  )}`;
  const close = () => {
    if (props.note) props.onNote(false);
    else props.onDismiss();
  };
  return (
    <Popover id={id} open modal={false} title="Selection" virtualAnchor={selection.bounds} onClose={close}>
      <View style={styles.column}>
        <Text style={styles.quote}>{selection.quote}</Text>
        {props.error && <Notice tone="danger">{props.error}</Notice>}
        {props.note ? <SelectionNote {...props} id={id} /> : <SelectionActions {...props} id={id} />}
        <Button id={`${id}:dismiss`} disabled={props.busy} onClick={props.onDismiss}>
          Close
        </Button>
      </View>
    </Popover>
  );
}
function SelectionActions(props: SelectionEditorProps & { id: string }) {
  return (
    <View style={styles.column}>
      <View style={styles.row}>
        {(['yellow', 'green', 'blue', 'pink'] as const).map((color) => (
          <Button
            key={color}
            id={`${props.id}:highlight:${color}`}
            title={`Highlight ${color}`}
            disabled={props.busy}
            onClick={() => props.onSave('highlight', color)}
          >
            {color}
          </Button>
        ))}
      </View>
      <Button id={`${props.id}:question`} disabled={props.busy} onClick={() => props.onSave('question', 'yellow')}>
        Didn't follow
      </Button>
      <Button id={`${props.id}:note`} disabled={props.busy} onClick={() => props.onNote(true)}>
        Add note
      </Button>
      {props.busy && <Text style={styles.muted}>Saving selection…</Text>}
    </View>
  );
}
function SelectionNote(props: SelectionEditorProps & { id: string }) {
  return (
    <View style={styles.column}>
      <Field
        id={`${props.id}:body`}
        label="Note"
        value={props.draft}
        onChange={props.onDraft}
        multiline
        rows={4}
        disabled={props.busy}
      />
      <View style={styles.row}>
        <Button
          id={`${props.id}:save`}
          tone="primary"
          disabled={props.busy}
          onClick={() => props.onSave('note', 'yellow')}
        >
          {props.busy ? 'Saving…' : 'Save'}
        </Button>
        <Button id={`${props.id}:cancel`} disabled={props.busy} onClick={() => props.onNote(false)}>
          Cancel
        </Button>
      </View>
    </View>
  );
}
