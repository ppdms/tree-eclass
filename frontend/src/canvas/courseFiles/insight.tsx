import * as React from 'react';
import type { FileInsight } from '@/lib/types';
import { AppLink } from '../app/links';
import { Button, Notice } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { FileGuide } from './guide';
import { GuideStatus } from './guideStatus';
import { fileIdentity } from './helpers';
import { useFileGuide } from './useFileGuide';

function InsightMeta({ insight }: { insight: FileInsight }) {
  return (
    <View style={{ flexDirection: 'row', flexWrap: 'wrap', gap: 12 }}>
      {insight.page_count ? (
        <Text style={{ color: colors.muted, fontSize: 12 }}>
          {insight.page_count} {insight.unit_name || 'pages'}
        </Text>
      ) : null}
      {insight.reading_minutes ? (
        <Text style={{ color: colors.muted, fontSize: 12 }}>~{insight.reading_minutes} min</Text>
      ) : null}
      {insight.complexity_label ? (
        <Text title="Text density" style={{ color: colors.muted, fontSize: 12 }}>
          {insight.complexity_label}
        </Text>
      ) : null}
    </View>
  );
}

function InsightActions({
  insight,
  open,
  onToggle,
  id,
}: {
  insight: FileInsight;
  open: boolean;
  onToggle: () => void;
  id: string;
}) {
  const canRead = insight.id && ['pdf', 'image'].includes(insight.document_kind || '');
  const readerQuery = new URLSearchParams({ course_id: String(insight.course_id), document_id: insight.id || '' });
  return (
    <View style={{ flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 12 }}>
      <GuideStatus insight={insight} />
      {canRead && (
        <AppLink
          id={`${id}-read`}
          href={`/study/session?${readerQuery}`}
          label="Read this file"
          style={{ color: colors.link, fontSize: 13 }}
        >
          Read
        </AppLink>
      )}
      {insight.guide_available || insight.ai?.summary ? (
        <Button id={`${id}-toggle`} expanded={open} controls={id} onClick={onToggle}>
          {open ? 'Close guide' : 'Guide'}
        </Button>
      ) : null}
    </View>
  );
}

function LazyFileGuide({ insight, open, id }: { insight: FileInsight; open: boolean; id: string }) {
  const { ai, loaded, error, retry } = useFileGuide(insight, open);
  if (!open) return null;
  if (error) {
    return (
      <Notice id={id} tone="danger">
        <Text>Guide could not load.</Text>
        <Button id={`${id}-retry`} onClick={retry}>
          Try again
        </Button>
      </Notice>
    );
  }
  if (!loaded) return <Text id={id}>Loading guide…</Text>;
  if (!ai) return <Text id={id}>No current study guide is available for this file.</Text>;
  return <FileGuide ai={ai} id={id} />;
}

export function Insight({
  insight,
  courseId,
  rowId,
}: {
  insight?: FileInsight;
  courseId: string | number;
  rowId: string;
}) {
  const [open, setOpen] = React.useState(false);
  const current = React.useMemo(
    () => (insight ? { ...insight, course_id: insight.course_id ?? courseId } : null),
    [insight, courseId],
  );
  if (!current) return null;
  const id = fileIdentity('file-guide', rowId, current.course_id, current.id);
  const available = Boolean(current.guide_available || current.ai?.summary);
  return (
    <View style={{ gap: 10, paddingLeft: 8, paddingTop: 6 }}>
      <View style={{ flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 12 }}>
        <InsightMeta insight={current} />
        <InsightActions insight={current} open={open && available} onToggle={() => setOpen(!open)} id={id} />
      </View>
      <LazyFileGuide insight={current} open={open && available} id={id} />
    </View>
  );
}
