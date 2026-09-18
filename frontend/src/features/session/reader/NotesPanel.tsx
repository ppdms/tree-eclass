import * as stylex from '@stylexjs/stylex';
import { useState } from 'react';
import { AnnotationList, type NotesPanelProps } from './NotesPanelCards';

const styles = stylex.create({
  panel: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
  },
});

export default function NotesPanel({ annotations, onUpdate, onDelete, onGoToPage }: NotesPanelProps) {
  const [editing, setEditing] = useState<string | number | null>(null);
  const [editingBody, setEditingBody] = useState('');
  const visible = annotations || [];
  return (
    <div {...stylex.props(styles.panel)}>
      <AnnotationList
        annotations={visible}
        editing={editing}
        editingBody={editingBody}
        onEditingBodyChange={setEditingBody}
        onStartEdit={(item) => {
          setEditing(item.id);
          setEditingBody(item.body || '');
        }}
        onSaveEdit={async (id) => {
          if (await onUpdate(id, { body: editingBody })) setEditing(null);
        }}
        onCancelEdit={() => setEditing(null)}
        onDelete={onDelete}
        onGoToPage={onGoToPage}
      />
    </div>
  );
}
