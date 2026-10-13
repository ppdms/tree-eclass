import { useEffect, useState } from 'react';
import { formatMinutes } from '@/features/session/components/formatMinutes';
import { AppLink } from '../app/links';
import { Button } from '../ui/controls';
import { Field, Range, Select } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import type { ReaderHandle, ReaderState } from './reader/types';
import { styles } from './styles';
import type { Context, DrawerTab, Source } from './types';

export interface ToolbarProps {
  context: Context;
  source: Source | null;
  reader: ReaderHandle | null;
  readerState: ReaderState;
  page: number;
  pageCount: number;
  seconds: number;
  paused: boolean;
  started: boolean;
  closed: boolean;
  marks: number;
  bookmarked: boolean;
  bookmarkBusy: boolean;
  selection: boolean;
  drawer: DrawerTab | null;
  onDocument: (id: string) => void;
  onPage: (page: number) => void;
  onDrawer: (tab: DrawerTab | null) => void;
  onBookmark: () => void;
  onNote: () => void;
  onHide: () => void;
  onFinish: () => void;
}
export function SessionToolbar(props: ToolbarProps) {
  const course = props.context.course.short_name || props.context.course.name || 'Study';
  return (
    <View id="session-toolbar" style={styles.panel}>
      <View style={styles.row}>
        <AppLink href="/study" style={styles.body}>
          Back to plan
        </AppLink>
        <Text style={styles.title}>{course}</Text>
        {props.context.documents.length > 1 ? (
          <Select
            id="session-document"
            label="Choose document"
            value={String(props.source?.document_id ?? '')}
            onChange={props.onDocument}
            options={props.context.documents.map((document) => ({
              value: String(document.document_id),
              label: document.display_name || document.file_name || String(document.document_id),
            }))}
          />
        ) : (
          <Text style={styles.source} title={props.source?.display_name}>
            {props.source?.display_name || 'Reading'}
          </Text>
        )}
        <Text style={{ ...styles.muted, color: props.paused ? colors.warning : colors.success }}>●</Text>
        <Text style={styles.muted}>
          {formatMinutes(props.seconds)}
          {props.paused ? ' (paused)' : ''}
        </Text>
      </View>
      <View
        id="session-toolbar-actions"
        scroll={{ axis: 'horizontal' }}
        style={{ ...styles.row, flexWrap: 'nowrap', width: '100%', overflow: 'hidden' }}
      >
        <ReaderControls {...props} />
        <PageNavigation
          page={props.page}
          count={props.pageCount}
          ready={props.readerState.ready}
          onPage={props.onPage}
        />
        <ToolbarActions {...props} />
      </View>
    </View>
  );
}
function ReaderControls(props: ToolbarProps) {
  const ready = props.readerState.ready;
  return (
    <View style={{ ...styles.row, flexWrap: 'nowrap', flexShrink: 0 }}>
      <Button id="session-zoom-out" title="Zoom out" disabled={!ready} onClick={() => props.reader?.zoomOut()}>
        −
      </Button>
      <Text style={styles.muted}>{props.readerState.zoom ? `${Math.round(props.readerState.zoom * 100)}%` : '—'}</Text>
      <Button id="session-zoom-in" title="Zoom in" disabled={!ready} onClick={() => props.reader?.zoomIn()}>
        +
      </Button>
      <Button
        id="session-fit-page"
        pressed={props.readerState.mode === 'page'}
        disabled={!ready}
        onClick={() => props.reader?.fitPage()}
      >
        Fit page
      </Button>
      <Button
        id="session-fit-width"
        pressed={props.readerState.mode === 'width'}
        disabled={!ready}
        onClick={() => props.reader?.fitWidth()}
      >
        Fit width
      </Button>
    </View>
  );
}
function PageNavigation(props: { page: number; count: number; ready: boolean; onPage: (page: number) => void }) {
  const [draft, setDraft] = useState(String(props.page));
  useEffect(() => setDraft(String(props.page)), [props.page]);
  const commit = () => {
    const requested = Number(draft);
    const page = Math.max(
      1,
      Math.min(props.count || 1, Number.isFinite(requested) ? Math.round(requested) : props.page),
    );
    setDraft(String(page));
    props.onPage(page);
  };
  return (
    <View style={{ ...styles.row, flexWrap: 'nowrap', flexShrink: 0 }}>
      <View style={styles.pageField}>
        <Field
          id="session-page"
          label="Page"
          type="number"
          value={draft}
          onChange={setDraft}
          onSubmit={commit}
          onBlur={commit}
          min={1}
          max={props.count || 1}
          disabled={!props.ready}
        />
      </View>
      <Text style={styles.muted}>/ {props.count || '—'}</Text>
      <Range
        id="session-page-range"
        label={`Page ${props.page} of ${props.count || 'unknown'}`}
        min={1}
        max={Math.max(1, props.count)}
        step={1}
        value={props.page}
        onChange={props.onPage}
        disabled={!props.ready}
      />
    </View>
  );
}
function ToolbarActions(props: ToolbarProps) {
  return (
    <View style={{ ...styles.row, flexWrap: 'nowrap', flexShrink: 0 }}>
      <Button
        id="session-inspector"
        expanded={props.drawer !== null}
        controls="session-drawer"
        onClick={() => props.onDrawer(props.drawer ? null : 'insight')}
      >
        Inspector · {props.marks}
      </Button>
      <Button
        id="session-bookmark"
        pressed={props.bookmarked}
        disabled={!props.source?.source_hash || props.bookmarkBusy}
        onClick={props.onBookmark}
      >
        {props.bookmarked ? 'Delete bookmark' : 'Bookmark page'}
      </Button>
      <Button id="session-add-note" disabled={!props.selection || props.bookmarkBusy} onClick={props.onNote}>
        Add note
      </Button>
      <Button id="session-finish" disabled={!props.started} onClick={props.onFinish}>
        {props.closed ? 'Recorded ✓' : 'Finish session'}
      </Button>
      <Button id="session-hide-controls" onClick={props.onHide}>
        Hide controls
      </Button>
    </View>
  );
}
