import { useState } from 'react';
import type { CoverageGap, QuestionFamily, RoadmapBlueprint, RoadmapConflict } from '@/lib/types';
import { readableValue } from '@/lib/display';
import { Button, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { DeferredSection } from './deferredSection';
import { Evidence } from './evidence';
import { semanticId, styles } from './styles';

type Roadmap = NonNullable<RoadmapBlueprint['blueprint']>;
interface SupportProps {
  roadmap: Roadmap;
  courseId: string | number;
  revision?: string;
  version: number;
  onRefresh: () => void;
}

function FamilyCard({ family, courseId, id }: { family: QuestionFamily; courseId: string | number; id: string }) {
  return (
    <Panel id={id} style={styles.card}>
      <Text style={styles.heading}>{readableValue(family.name, 'Question family')}</Text>
      <Text style={styles.meta}>
        {readableValue(family.priority)} · {String(family.response_mode || '').replaceAll('_', ' ')} ·{' '}
        {readableValue(family.confidence)} confidence
      </Text>
      <Text style={styles.meta}>
        Observed {family.observed_count ?? 0}/{family.observed_out_of ?? 0} papers
        {family.estimated_marks_percent ? ` · ~${family.estimated_marks_percent}% of marks` : ''}
      </Text>
      <Evidence id={`${id}:sources`} links={family.evidence_links} courseId={courseId} />
    </Panel>
  );
}

function ConflictCard({
  conflict,
  courseId,
  id,
}: {
  conflict: RoadmapConflict;
  courseId: string | number;
  id: string;
}) {
  return (
    <Panel id={id} style={styles.card}>
      <Text style={styles.heading}>Conflict</Text>
      <Text style={styles.body}>{readableValue(conflict.summary)}</Text>
      {conflict.status && <Text style={styles.meta}>Status: {readableValue(conflict.status)}</Text>}
      {conflict.resolution && <Text style={styles.meta}>Resolution: {readableValue(conflict.resolution)}</Text>}
      <Evidence id={`${id}:sources`} links={conflict.evidence_links} courseId={courseId} />
    </Panel>
  );
}

function GapCard({ gap, courseId, id }: { gap: CoverageGap; courseId: string | number; id: string }) {
  return (
    <Panel id={id} style={styles.card}>
      <Text style={styles.heading}>Coverage gap</Text>
      <Text style={styles.body}>{readableValue(gap.summary)}</Text>
      {gap.recommended_action && <Text style={styles.meta}>Next step: {readableValue(gap.recommended_action)}</Text>}
      <Evidence id={`${id}:sources`} links={gap.evidence_links} courseId={courseId} />
    </Panel>
  );
}

function SupportContent({ roadmap, courseId, revision }: Omit<SupportProps, 'onRefresh'>) {
  const id = semanticId('roadmap', courseId, revision, 'support');
  return (
    <View style={styles.stack}>
      {(roadmap.question_families || []).map((family) => {
        const identity = semanticId(
          id,
          'family',
          family.key || family.name,
          family.response_mode,
          family.unit_keys?.join('|'),
          family.evidence_refs?.join('|'),
        );
        return <FamilyCard key={identity} id={identity} family={family} courseId={courseId} />;
      })}
      {(roadmap.conflicts || []).map((conflict) => {
        const identity = semanticId(
          id,
          'conflict',
          conflict.summary,
          conflict.status,
          conflict.resolution,
          conflict.evidence_refs?.join('|'),
        );
        return <ConflictCard key={identity} id={identity} conflict={conflict} courseId={courseId} />;
      })}
      {(roadmap.coverage_gaps || []).map((gap) => {
        const identity = semanticId(
          id,
          'gap',
          gap.summary,
          gap.severity,
          gap.recommended_action,
          gap.evidence_refs?.join('|'),
        );
        return <GapCard key={identity} id={identity} gap={gap} courseId={courseId} />;
      })}
    </View>
  );
}

export function SupportSection({ roadmap, ...props }: SupportProps) {
  const [open, setOpen] = useState(false);
  const hasContent = roadmap.question_families?.length || roadmap.conflicts?.length || roadmap.coverage_gaps?.length;
  if (!hasContent && !roadmap.deferred_sections?.support) return null;
  const id = semanticId('roadmap', props.courseId, props.revision, 'support');
  return (
    <Panel style={styles.card}>
      <Button id={`${id}:toggle`} expanded={open} controls={`${id}:body`} onClick={() => setOpen(!open)}>
        Exam patterns, conflicts, and evidence gaps
      </Button>
      <View id={`${id}:body`}>
        {roadmap.deferred_sections?.support ? (
          <DeferredSection {...props} id={id} section="support" open={open}>
            {(value) => <SupportContent {...props} roadmap={value} />}
          </DeferredSection>
        ) : (
          open && <SupportContent {...props} roadmap={roadmap} />
        )}
      </View>
    </Panel>
  );
}
