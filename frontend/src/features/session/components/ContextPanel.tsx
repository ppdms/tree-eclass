import * as stylex from '@stylexjs/stylex';
import type { Dispatch, SetStateAction } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { colors } from '@/styles/tokens.stylex';

import InsightPanel from '@/features/session/reader/InsightPanel';
import NotesPanel from '@/features/session/reader/NotesPanel';
import { DeferredRecall } from './DeferredRecall';
import type { Annotation, PracticeView } from '@/features/session/reader/types';
import type { AnnotationUpdatePayload } from '@/features/session/reader/api';
import type { DrawerTab } from './useSessionState';

const TABS: [DrawerTab, string][] = [
  ['insight', 'Insight'],
  ['notes', 'Marks'],
  ['recall', 'Recall'],
];

const styles = stylex.create({
  container: {
    overflow: 'hidden',
    display: 'flex',
    flexBasis: '0%',
    flexDirection: 'column',
    flexGrow: 1,
    flexShrink: 1,
  },
  tabsRoot: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
  },
  tabsList: {
    padding: 0,
    borderRadius: 0,
    backgroundColor: colors.surface,
    flexBasis: 'auto',
    flexGrow: 0,
    flexShrink: 0,
    borderBottomColor: colors.border,
    borderBottomWidth: '1px',
    width: '100%',
  },
  tabTrigger: {
    borderRadius: 0,
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    fontSize: '0.75rem',
  },
  tabContentAuto: {
    overflow: 'auto',
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    marginTop: 0,
    minHeight: 0,
  },
  tabContentHidden: {
    overflow: 'hidden',
    flexBasis: '0%',
    flexGrow: 1,
    flexShrink: 1,
    marginTop: 0,
    minHeight: 0,
  },
});

function PanelTabs({ annotations }: { annotations: Annotation[] }) {
  const visibleCount = annotations.filter((item) => item.status !== 'deleted').length;
  return (
    <TabsList {...stylex.props(styles.tabsList)}>
      {TABS.map(([value, label]) => (
        <TabsTrigger key={value} value={value} {...stylex.props(styles.tabTrigger)}>
          {label}
          {value === 'notes' && visibleCount ? ` (${visibleCount})` : ''}
        </TabsTrigger>
      ))}
    </TabsList>
  );
}

export interface ContextPanelProps {
  tab: DrawerTab;
  onTabChange: Dispatch<SetStateAction<DrawerTab | null>>;
  annotations: Annotation[];
  courseId: string | number;
  documentId?: string | number | null;
  pageNumber: number;
  onUpdate: (id: string | number, patch: AnnotationUpdatePayload) => Promise<Annotation | null>;
  onDelete: (id: string | number) => Promise<boolean>;
  onGoToPage?: (page: number) => void;
  practice: PracticeView | null;
  unitKey?: string;
  onRecorded?: (practice: PracticeView) => void;
}

function PanelTabsContent(props: ContextPanelProps) {
  const { annotations, courseId, documentId, pageNumber, onUpdate, onDelete, onGoToPage } = props;
  return (
    <>
      <TabsContent value="insight" {...stylex.props(styles.tabContentAuto)}>
        <InsightPanel courseId={courseId} documentId={documentId} pageNumber={pageNumber} />
      </TabsContent>

      <TabsContent value="notes" {...stylex.props(styles.tabContentHidden)}>
        <NotesPanel annotations={annotations} onUpdate={onUpdate} onDelete={onDelete} onGoToPage={onGoToPage} />
      </TabsContent>

      <TabsContent value="recall" {...stylex.props(styles.tabContentHidden)}>
        <DeferredRecall {...props} />
      </TabsContent>
    </>
  );
}

function PanelBody(props: ContextPanelProps) {
  const { tab, onTabChange, annotations } = props;
  return (
    <Tabs
      value={tab}
      onValueChange={(value) =>
        // SAFETY: tab change events only originate from the predefined PanelTabs triggers;
        // the cast narrows string to DrawerTab.
        onTabChange(value as DrawerTab)
      }
      {...stylex.props(styles.tabsRoot)}
    >
      <PanelTabs annotations={annotations} />
      <PanelTabsContent {...props} />
    </Tabs>
  );
}

export default function ContextPanel(props: ContextPanelProps) {
  return (
    <div {...stylex.props(styles.container)}>
      <PanelBody {...props} />
    </div>
  );
}
