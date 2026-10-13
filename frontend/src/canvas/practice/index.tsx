import { useState } from 'react';
import { Button, Notice, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { semanticId, styles } from '../roadmap/styles';
import { PracticeQuestionCard } from './card';
import { usePracticeSession } from './useSession';
import type { PracticeSession } from './useSession';
import { usePracticeView, type PracticeViewResource } from './useView';

export interface CoursePracticeProps {
  courseId: string | number;
  practice: { enabled?: boolean; pending_units?: number };
  revision?: string;
  version: number;
  onRefresh: () => void;
}

export function PracticeDisclosure(props: CoursePracticeProps) {
  const [open, setOpen] = useState(false);
  const id = semanticId('practice', props.courseId, props.revision);
  if (!props.practice.enabled) return null;
  return (
    <Panel style={styles.card}>
      <Button id={`${id}:toggle`} expanded={open} controls={`${id}:body`} onClick={() => setOpen(!open)}>
        Practice questions
      </Button>
      {open && (
        <View id={`${id}:body`}>
          <PracticePanel key={id} {...props} />
        </View>
      )}
    </Panel>
  );
}

function PracticeHeader({ pending, due, questions }: { pending: number; due: number; questions: number }) {
  return (
    <View style={styles.stack}>
      <Text style={styles.title}>Active recall</Text>
      <Text style={styles.body}>
        Answer from memory first, then reveal the expected answer and grade yourself honestly. Every attempt is kept.
      </Text>
      <View style={styles.row}>
        <Text style={styles.meta}>{questions ? `${due} due of ${questions}` : 'no questions yet'}</Text>
        {pending > 0 && (
          <Text style={styles.meta}>
            {pending} unit{pending === 1 ? '' : 's'} preparing
          </Text>
        )}
      </View>
    </View>
  );
}

interface QueueProps {
  id: string;
  session: PracticeSession;
  resource: PracticeViewResource;
}

function PracticeQueue({ id, session, resource }: QueueProps) {
  if (resource.error) return <Notice tone="danger">{resource.error}</Notice>;
  if (!resource.view) return <Text style={styles.meta}>Opening the practice queue…</Text>;
  const question = session.queue[session.index];
  return question ? (
    <PracticeQuestionCard
      id={id}
      question={question}
      index={session.index}
      queueLength={session.queue.length}
      revealed={session.revealed}
      busy={session.busy}
      disabled={session.stale || resource.view.enabled === false}
      note={session.note}
      onReveal={() => session.setRevealed(true)}
      onNote={session.setNote}
      onNext={session.next}
      onGrade={(outcome) => {
        void session.record(outcome);
      }}
    />
  ) : (
    <Text style={styles.meta}>
      {session.queue.length
        ? 'Questions for this course are still being written. They appear here automatically.'
        : 'No practice questions are available for the current roadmap revision yet.'}
    </Text>
  );
}

export function PracticePanel({ courseId, practice, revision, version, onRefresh }: CoursePracticeProps) {
  const resource = usePracticeView(courseId, version);
  const session = usePracticeSession(
    courseId,
    resource.view,
    resource.setView,
    revision,
    resource.generation,
    resource.current,
  );
  const id = semanticId('practice', courseId, resource.view?.blueprint_revision_id || revision);
  const totals = resource.view?.totals || {};
  return (
    <View id={id} style={styles.stack}>
      <PracticeHeader
        pending={resource.view?.pending_units ?? practice.pending_units ?? 0}
        due={totals.due || 0}
        questions={totals.questions || 0}
      />
      <PracticeQueue id={id} session={session} resource={resource} />
      <PracticeNotices session={session} disabled={resource.view?.enabled === false} />
      {session.stale && (
        <Button id={`${id}:refresh-roadmap`} onClick={onRefresh}>
          Refresh roadmap
        </Button>
      )}
      {resource.view && (
        <Text style={styles.meta}>
          {totals.due || 0} due · {totals.learned || 0} learned · {totals.questions || 0} total
        </Text>
      )}
      <Text style={styles.meta}>
        Questions are AI-written from the evidence each roadmap unit cites, and are replaced whenever that roadmap
        revision changes. Your attempt history is kept separately and survives every regeneration. Check the linked
        material before trusting an answer.
      </Text>
    </View>
  );
}

function PracticeNotices({ session, disabled }: { session: PracticeSession; disabled: boolean }) {
  return (
    <View style={styles.stack}>
      {session.busy && <Text style={styles.meta}>Recording attempt…</Text>}
      {session.status && <Notice tone="success">{session.status}</Notice>}
      {session.error && <Notice tone="danger">{session.error}</Notice>}
      {session.error && session.hasRetry && !session.stale && (
        <Notice>Retrying the same grade uses its original note and request ID.</Notice>
      )}
      {session.refreshError && <Notice tone="warning">{session.refreshError}</Notice>}
      {session.stale && (
        <Notice tone="warning">
          The practice revision changed. Refresh the roadmap before opening the current questions.
        </Notice>
      )}
      {disabled && <Notice tone="warning">Practice is currently disabled. No attempt will be recorded.</Notice>}
    </View>
  );
}

export type { CoursePracticeView, PracticeOutcome } from './model';
