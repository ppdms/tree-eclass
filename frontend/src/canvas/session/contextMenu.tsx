import { useEffect, useState } from 'react';
import { Button, Notice } from '../ui/controls';
import { Popover } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { message } from './api';
import { styles } from './styles';
import type { Selection } from './types';

export interface ReaderMenu {
  x: number;
  y: number;
  page: number;
  selection: Selection | null;
}
interface ReaderContextMenuProps {
  menu: ReaderMenu | null;
  count: number;
  bookmarked: boolean;
  controls: boolean;
  busy: boolean;
  onClose: () => void;
  onHighlight: (selection: Selection) => void;
  onQuestion: (selection: Selection) => void;
  onNote: (selection: Selection) => void;
  onBookmark: (page: number) => void;
  onControls: () => void;
}
interface SelectionMenuProps {
  selection: Selection;
  props: ReaderContextMenuProps;
  act: (action: () => void) => void;
  onCopyError: (error: string) => void;
}
function SelectionMenu({ selection, props, act, onCopyError }: SelectionMenuProps) {
  return (
    <>
      <Button
        id="session-context-highlight"
        disabled={props.busy}
        onClick={() => act(() => props.onHighlight(selection))}
      >
        Highlight selection
      </Button>
      <Button id="session-context-note" disabled={props.busy} onClick={() => act(() => props.onNote(selection))}>
        Add note
      </Button>
      <Button
        id="session-context-question"
        disabled={props.busy}
        onClick={() => act(() => props.onQuestion(selection))}
      >
        Mark as didn't follow
      </Button>
      <Button
        id="session-context-copy"
        copyText={selection.quote}
        onCopyResult={(failure) => {
          if (failure === null) props.onClose();
          else onCopyError(message(failure));
        }}
      >
        Copy selection
      </Button>
    </>
  );
}
export function ReaderContextMenu(props: ReaderContextMenuProps) {
  const [error, setError] = useState<string | null>(null);
  useEffect(() => setError(null), [props.menu]);
  const menu = props.menu;
  if (!menu) return null;
  const selection = menu.selection;
  const act = (action: () => void) => {
    props.onClose();
    setError(null);
    action();
  };
  return (
    <Popover
      id="session-context-menu"
      title="Reader menu"
      role="menu"
      open
      virtualAnchor={{ x: menu.x, y: menu.y, width: 1, height: 1 }}
      onClose={props.onClose}
    >
      <View style={styles.column}>
        <Text style={styles.title}>
          Page {menu.page} of {props.count || 'unknown'}
        </Text>
        {selection && <SelectionMenu selection={selection} props={props} act={act} onCopyError={setError} />}
        <Button
          id="session-context-bookmark"
          disabled={props.busy}
          onClick={() => act(() => props.onBookmark(menu.page))}
        >
          {props.bookmarked ? 'Delete bookmark' : 'Bookmark this page'}
        </Button>
        <Button id="session-context-controls" onClick={() => act(props.onControls)}>
          {props.controls ? 'Hide controls' : 'Show controls'}
        </Button>
        {error && <Notice tone="danger">Copy failed: {error}</Notice>}
      </View>
    </Popover>
  );
}
