import * as stylex from '@stylexjs/stylex';
import { colors, typography } from '@/styles/tokens.stylex';
import { AnnotationCard, type AnnotationListProps } from './NotesPanelCards';

const styles = stylex.create({
  listContainer: {
    paddingInline: '1rem',
    paddingBlockEnd: '1.5rem',
    paddingBlockStart: '0.25rem',
  },
  emptyState: {
    paddingBlock: '3rem',
    paddingInline: '0.75rem',
    color: colors.textSecondary,
    textAlign: 'center',
  },
  emptyTitle: {
    margin: 0,
    color: colors.textPrimarySoft,
    fontSize: '0.8125rem',
    marginBottom: '0.25rem',
  },
  emptyDesc: {
    display: 'block',
    fontSize: '0.75rem',
    lineHeight: typography.leadingRelaxed,
  },
  itemsList: {
    margin: 0,
    padding: 0,
    listStyle: 'none',
  },
});

export function AnnotationListContent({
  annotations,
  editing,
  editingBody,
  onEditingBodyChange,
  onStartEdit,
  onSaveEdit,
  onCancelEdit,
  onDelete,
  onGoToPage,
}: AnnotationListProps) {
  return (
    <div {...stylex.props(styles.listContainer)}>
      {annotations.length ? (
        <AnnotationItems
          annotations={annotations}
          editing={editing}
          editingBody={editingBody}
          onEditingBodyChange={onEditingBodyChange}
          onStartEdit={onStartEdit}
          onSaveEdit={onSaveEdit}
          onCancelEdit={onCancelEdit}
          onDelete={onDelete}
          onGoToPage={onGoToPage}
        />
      ) : (
        <AnnotationEmpty />
      )}
    </div>
  );
}

function AnnotationEmpty() {
  return (
    <div {...stylex.props(styles.emptyState)}>
      <p {...stylex.props(styles.emptyTitle)}>No marks on this document yet.</p>
      <span {...stylex.props(styles.emptyDesc)}>
        Select text to highlight it, or bookmark the page from the toolbar.
      </span>
    </div>
  );
}

function AnnotationItems(props: AnnotationListProps) {
  return (
    <ul {...stylex.props(styles.itemsList)}>
      {props.annotations.map((item) => (
        <li key={item.id}>
          <AnnotationCard
            item={item}
            editing={props.editing === item.id}
            editingBody={props.editingBody}
            onEditingBodyChange={props.onEditingBodyChange}
            onStartEdit={() => props.onStartEdit(item)}
            onSaveEdit={() => props.onSaveEdit(item.id)}
            onCancelEdit={props.onCancelEdit}
            onDelete={props.onDelete}
            onGoToPage={props.onGoToPage}
          />
        </li>
      ))}
    </ul>
  );
}
