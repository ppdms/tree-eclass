import { annotationAnchorId } from './reader/types';
import { Button, Notice } from '../ui/controls';
import { Field } from '../ui/forms';
import { Popover } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';
import type { MarkActions } from './useMarks';
import type { Mark } from './types';

export interface MarkEditor {
  id: string;
  body: string;
}
interface MarksProps {
  actions: MarkActions;
  editor: MarkEditor | null;
  onEditor: (editor: MarkEditor | null) => void;
  onPage: (page: number) => void;
}
export function MarksPanel(props: MarksProps) {
  if (props.actions.loading) return <Text style={styles.muted}>Loading marks…</Text>;
  if (props.actions.readError)
    return (
      <View style={styles.column}>
        <Notice tone="danger">Marks could not be loaded: {props.actions.readError}</Notice>
        <Button id="session-marks-retry" onClick={props.actions.retry}>
          Try again
        </Button>
      </View>
    );
  if (!props.actions.marks.length)
    return (
      <Text style={styles.muted}>
        No marks yet. Select some text to highlight, add a note or question, or bookmark a page.
      </Text>
    );
  return (
    <View style={styles.column}>
      {props.actions.marks.map((mark) => (
        <MarkCard key={String(mark.id)} mark={mark} {...props} />
      ))}
    </View>
  );
}
function MarkNoteButton({
  id,
  mark,
  busy,
  onEditor,
}: {
  id: string;
  mark: Mark;
  busy: boolean;
  onEditor: MarksProps['onEditor'];
}) {
  return (
    <Button
      id={`${id}:edit`}
      disabled={busy}
      style={{ justifyContent: 'start' }}
      onClick={() => onEditor({ id: String(mark.id), body: mark.body ?? '' })}
    >
      {/* Note bodies remain readable wrapping content, not an unconstrained button caption. */}
      <Text
        style={{
          minWidth: 0,
          flexGrow: 1,
          flexShrink: 1,
          textAlign: 'left',
          fontSize: 12,
          fontWeight: 400,
          lineHeight: 1.55,
        }}
      >
        {mark.body || 'Add a note'}
      </Text>
    </Button>
  );
}
function MarkCard(props: MarksProps & { mark: Mark }) {
  const { mark, actions, editor, onEditor } = props;
  const id = `session:mark:${encodeURIComponent(String(mark.document_id ?? ''))}:${mark.id}`;
  const busy = actions.busy.has(`mark:${mark.id}`);
  const editing = editor?.id === String(mark.id);
  const save = async () => {
    if (editor && (await actions.update(mark, { body: editor.body }))) onEditor(null);
  };
  return (
    <View id={id} style={{ ...styles.mark, opacity: mark.status === 'deleted' ? 0.6 : 1 }}>
      <View style={styles.row}>
        <Button id={`${id}:page`} onClick={() => props.onPage(mark.page_number)}>
          Page {mark.page_number}
        </Button>
        <Text style={styles.title}>{mark.kind}</Text>
        {mark.status === 'deleted' && <Text style={styles.muted}>Deleted history</Text>}
      </View>
      {mark.status === 'orphaned' && <Notice tone="warning">Older version: this source has changed.</Notice>}
      {mark.quote && <Text style={styles.quote}>{mark.quote}</Text>}
      {editing ? (
        <MarkBodyEditor
          id={id}
          value={editor.body}
          busy={busy}
          onChange={(body) => onEditor({ id: String(mark.id), body })}
          onSave={() => {
            void save();
          }}
          onCancel={() => onEditor(null)}
        />
      ) : (
        <MarkNoteButton id={id} mark={mark} busy={busy} onEditor={onEditor} />
      )}
      <Button
        id={`${id}:delete`}
        tone="danger"
        disabled={busy}
        onClick={() => {
          void actions.remove(mark);
        }}
      >
        {busy ? 'Saving…' : 'Delete'}
      </Button>
    </View>
  );
}
function MarkBodyEditor(props: {
  id: string;
  value: string;
  busy: boolean;
  onChange: (body: string) => void;
  onSave: () => void;
  onCancel: () => void;
}) {
  return (
    <View style={styles.column}>
      <Field
        id={`${props.id}:body`}
        label="Annotation note"
        value={props.value}
        onChange={props.onChange}
        multiline
        rows={4}
        disabled={props.busy}
      />
      <View style={styles.row}>
        <Button id={`${props.id}:save`} disabled={props.busy} onClick={props.onSave}>
          Save
        </Button>
        <Button id={`${props.id}:cancel`} disabled={props.busy} onClick={props.onCancel}>
          Cancel
        </Button>
      </View>
    </View>
  );
}
function MarkColors({ mark, actions, busy }: { mark: Mark; actions: MarkActions; busy: boolean }) {
  return (
    <View style={styles.row}>
      {(['yellow', 'green', 'blue', 'pink'] as const).map((color) => (
        <Button
          key={color}
          id={`session:inline:${mark.id}:${color}`}
          pressed={mark.color === color}
          disabled={busy}
          onClick={() => {
            void actions.update(mark, { color });
          }}
        >
          {color}
        </Button>
      ))}
    </View>
  );
}
export function InlineMark({
  mark,
  actions,
  onClose,
}: {
  mark: Mark | null;
  actions: MarkActions;
  onClose: () => void;
}) {
  if (!mark) return null;
  const busy = actions.busy.has(`mark:${mark.id}`);
  const remove = async () => {
    if (await actions.remove(mark)) onClose();
  };
  return (
    <Popover
      id={`session:inline:${mark.id}`}
      title={`${mark.kind} · Page ${mark.page_number}`}
      open
      anchorId={annotationAnchorId(mark)}
      onClose={onClose}
    >
      <View style={styles.column}>
        {mark.quote && <Text style={styles.quote}>{mark.quote}</Text>}
        {mark.kind !== 'highlight' && <Text style={styles.body}>{mark.body || 'No note text yet'}</Text>}
        {mark.kind === 'highlight' && <MarkColors mark={mark} actions={actions} busy={busy} />}
        {actions.error && <Notice tone="danger">{actions.error}</Notice>}
        <Button
          id={`session:inline:${mark.id}:delete`}
          tone="danger"
          disabled={busy}
          onClick={() => {
            void remove();
          }}
        >
          Delete
        </Button>
        <Button id={`session:inline:${mark.id}:close`} onClick={onClose}>
          Close
        </Button>
      </View>
    </Popover>
  );
}
