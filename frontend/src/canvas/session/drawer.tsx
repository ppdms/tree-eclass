import type { ReactNode } from 'react';
import { Button, Panel } from '../ui/controls';
import { Dialog, FocusScope, ScrollArea } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';
import type { DrawerTab } from './types';

export interface DrawerProps {
  tab: DrawerTab | null;
  page: number;
  marks: number;
  overlay: boolean;
  height: number;
  onTab: (tab: DrawerTab) => void;
  onClose: () => void;
  insight: ReactNode;
  markList: ReactNode;
  recall: ReactNode;
}
export function SessionDrawer(props: DrawerProps) {
  if (!props.tab) return null;
  const body = <DrawerContent {...props} />;
  if (props.overlay)
    return (
      <Dialog id="session-drawer" title={`Page ${props.page} · ${props.tab}`} open onClose={props.onClose}>
        {body}
      </Dialog>
    );
  return (
    <FocusScope id="session-drawer" open onClose={props.onClose}>
      <Panel style={{ ...styles.drawer, height: props.height }}>{body}</Panel>
    </FocusScope>
  );
}
function DrawerContent(props: DrawerProps) {
  const title = props.tab === 'marks' ? 'Marks' : props.tab === 'recall' ? 'Recall' : 'Insight';
  return (
    <View style={styles.column}>
      <View style={styles.row}>
        <Text style={styles.title}>
          {title} · Page {props.page}
        </Text>
        <Button id="session-drawer-close" onClick={props.onClose}>
          Close
        </Button>
      </View>
      <Text style={styles.muted}>
        {props.marks} mark{props.marks === 1 ? '' : 's'} saved
      </Text>
      <View style={styles.row}>
        {(['insight', 'marks', 'recall'] as const).map((tab) => (
          <Button
            key={tab}
            id={`session-drawer-tab:${tab}`}
            pressed={props.tab === tab}
            controls={`session-drawer-panel:${tab}`}
            onClick={() => props.onTab(tab)}
          >
            {tab === 'marks' ? `Marks (${props.marks})` : tab === 'recall' ? 'Recall' : 'Insight'}
          </Button>
        ))}
      </View>
      <ScrollArea id={`session-drawer-scroll:${props.tab}`} height={Math.max(140, props.height - 160)}>
        <View id={`session-drawer-panel:${props.tab}`} style={styles.column}>
          {props.tab === 'insight' ? props.insight : props.tab === 'marks' ? props.markList : props.recall}
        </View>
      </ScrollArea>
    </View>
  );
}
