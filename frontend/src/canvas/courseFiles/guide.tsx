import * as React from 'react';
import type { FileInsightAi } from '@/lib/types';
import { readableValue } from '@/lib/display';
import { AppLink } from '../app/links';
import { Button, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { fileHref, fileIdentity } from './helpers';

function GuideList({ title, values = [] }: { title: string; values?: string[] }) {
  if (!values.length) return null;
  return (
    <View style={{ gap: 6 }}>
      <Text style={{ fontWeight: 650 }}>{title}</Text>
      {values.map((value, index) => (
        <Text key={`${title}-${index}`} style={{ color: colors.muted }}>
          • {readableValue(value)}
        </Text>
      ))}
    </View>
  );
}

function GuideSignals({ ai }: { ai: FileInsightAi }) {
  const signals = [
    ['Priority', ai.importance],
    ['Difficulty', ai.difficulty],
    ['Assessment', ai.assessment_relevance],
    ['Material', ai.material_type],
  ].filter((signal): signal is [string, string] => Boolean(signal[1]));
  if (!signals.length) return null;
  return (
    <View label="Study signals" style={{ flexDirection: 'row', flexWrap: 'wrap', gap: 12 }}>
      {signals.map(([label, value]) => (
        <Text key={label} style={{ fontSize: 12, color: colors.muted }}>
          {label}: {readableValue(value).replaceAll('_', ' ')}
        </Text>
      ))}
    </View>
  );
}

function RelatedMaterials({ id, items = [] }: { id: string; items?: FileInsightAi['related_materials'] }) {
  if (!items.length) return null;
  return (
    <View style={{ gap: 6 }}>
      <Text style={{ fontWeight: 650 }}>Related material</Text>
      {items.map((item) =>
        item.path ? (
          <AppLink
            key={fileIdentity('related', item.path, item.name)}
            id={fileIdentity('related', id, item.path, item.name)}
            href={fileHref(item.path)}
            target="_blank"
            rel="noopener noreferrer"
            style={{ color: colors.link }}
          >
            {readableValue(item.name)}
          </AppLink>
        ) : (
          <Text key={fileIdentity('related-name', item.name)}>{readableValue(item.name)}</Text>
        ),
      )}
    </View>
  );
}

function GuideMore({ ai, id }: { ai: FileInsightAi; id: string }) {
  const [open, setOpen] = React.useState(false);
  const hasMore = Boolean(
    ai.prerequisites?.length ||
    ai.visual_content?.length ||
    ai.notable_items?.length ||
    ai.related_materials?.length ||
    ai.difficulty_reason ||
    ai.overlap,
  );
  if (!hasMore) return null;
  return (
    <View style={{ gap: 10 }}>
      <Button id={`${id}-more`} expanded={open} controls={`${id}-context`} onClick={() => setOpen((value) => !value)}>
        {open ? 'Less context' : 'More context'}
      </Button>
      {open && (
        <View id={`${id}-context`} style={{ gap: 12 }}>
          <GuideList title="Prerequisites" values={ai.prerequisites} />
          <GuideList title="Visuals worth reviewing" values={ai.visual_content} />
          <GuideList title="Key details" values={ai.notable_items} />
          {ai.difficulty_reason && <Text>{readableValue(ai.difficulty_reason)}</Text>}
          {ai.overlap && <Text>{readableValue(ai.overlap)}</Text>}
          <RelatedMaterials id={id} items={ai.related_materials} />
        </View>
      )}
    </View>
  );
}

function GuideCore({ ai }: { ai: FileInsightAi }) {
  const examLens = ai.assessment_reason || ai.importance_reason || ai.course_role;
  return (
    <View style={{ gap: 12 }}>
      <GuideList title="Learn" values={ai.learning_objectives?.slice(0, 6)} />
      {examLens && (
        <View style={{ gap: 6 }}>
          <Text style={{ fontWeight: 650 }}>Exam lens</Text>
          <Text>{readableValue(examLens)}</Text>
        </View>
      )}
      {ai.topics?.length ? (
        <View label="Topics" style={{ flexDirection: 'row', flexWrap: 'wrap', gap: 8 }}>
          {ai.topics.slice(0, 10).map((topic) => (
            <Text key={topic} style={{ color: colors.muted, fontSize: 12 }}>
              {readableValue(topic)}
            </Text>
          ))}
        </View>
      ) : null}
    </View>
  );
}

export function FileGuide({ ai, id }: { ai: FileInsightAi; id: string }) {
  return (
    <Panel id={id} style={{ gap: 14, backgroundColor: colors.raised }}>
      {ai.summary && <Text>{readableValue(ai.summary)}</Text>}
      <GuideSignals ai={ai} />
      {ai.recommended_action && (
        <View style={{ gap: 6 }}>
          <Text style={{ fontWeight: 650, color: colors.accent }}>Do this next</Text>
          <Text>{readableValue(ai.recommended_action)}</Text>
        </View>
      )}
      <GuideCore ai={ai} />
      <GuideMore ai={ai} id={id} />
    </Panel>
  );
}
