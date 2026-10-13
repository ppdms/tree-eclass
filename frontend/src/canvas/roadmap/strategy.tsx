import { useState } from 'react';
import type { RoadmapBlueprint } from '@/lib/types';
import { readableValue } from '@/lib/display';
import { Button, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { DeferredSection } from './deferredSection';
import { Evidence } from './evidence';
import { semanticId, styles } from './styles';

type Roadmap = NonNullable<RoadmapBlueprint['blueprint']>;
type Strategy = NonNullable<Roadmap['exam_strategy']>;
interface StrategyProps {
  courseId: string | number;
  revision?: string;
  version: number;
  onRefresh: () => void;
}

function StrategyEvidence({
  courseId,
  revision,
  version,
  onRefresh,
  strategy,
}: StrategyProps & { strategy: Strategy }) {
  const [open, setOpen] = useState(false);
  const id = semanticId('roadmap', courseId, revision, 'strategy-evidence');
  return (
    <View style={styles.stack}>
      <Button id={`${id}:toggle`} expanded={open} controls={`${id}:body`} onClick={() => setOpen(!open)}>
        Show the evidence behind this strategy
      </Button>
      <View id={`${id}:body`}>
        {revision ? (
          <DeferredSection
            id={id}
            courseId={courseId}
            revision={revision}
            version={version}
            section="strategy-evidence"
            open={open}
            onRefresh={onRefresh}
          >
            {(roadmap) => (
              <Evidence id={`${id}:sources`} links={roadmap.exam_strategy?.evidence_links} courseId={courseId} />
            )}
          </DeferredSection>
        ) : (
          open && <Evidence id={`${id}:sources`} links={strategy.evidence_links} courseId={courseId} />
        )}
      </View>
    </View>
  );
}

function StrategyContent({ strategy, ...props }: StrategyProps & { strategy: Strategy }) {
  return (
    <View style={styles.stack}>
      <Text style={styles.meta}>
        Exam strategy · {readableValue(String(strategy.confidence ?? 'Unknown'))} confidence
      </Text>
      <Text style={styles.heading}>{readableValue(strategy.objective)}</Text>
      <Text style={styles.body}>{readableValue(strategy.approach)}</Text>
      <Text style={styles.meta}>
        Target reflected by this revision:{' '}
        {strategy.target_score_percent != null ? `${strategy.target_score_percent}%` : 'not specified'}
      </Text>
      <StrategyEvidence {...props} strategy={strategy} />
    </View>
  );
}

export function StrategySection({ roadmap, ...props }: StrategyProps & { roadmap: Roadmap }) {
  const [open, setOpen] = useState(false);
  if (!roadmap.exam_strategy && !roadmap.deferred_sections?.strategy) return null;
  const id = semanticId('roadmap', props.courseId, props.revision, 'strategy');
  return (
    <Panel style={styles.card}>
      <Button id={`${id}:toggle`} expanded={open} controls={`${id}:body`} onClick={() => setOpen(!open)}>
        Exam strategy and evidence
      </Button>
      <View id={`${id}:body`}>
        {roadmap.deferred_sections?.strategy ? (
          <DeferredSection {...props} id={id} section="strategy" open={open}>
            {(value) => value.exam_strategy && <StrategyContent {...props} strategy={value.exam_strategy} />}
          </DeferredSection>
        ) : (
          open && roadmap.exam_strategy && <StrategyContent {...props} strategy={roadmap.exam_strategy} />
        )}
      </View>
    </Panel>
  );
}
