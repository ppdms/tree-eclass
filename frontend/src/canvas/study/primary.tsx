import { useState } from 'react';
import type { StudyAction } from '@/lib/types';
import { reloadPage } from '../app/browser';
import { AppLink } from '../app/links';
import { StudyLevel } from '../courseFiles/level';
import { Button, Notice, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { StudyEvidence } from './evidence';
import { actionIdentity, conciseInstruction, fallbackHref } from './model';
import { RecordOutcome } from './outcome';
import type { OutcomeResponse } from './outcomeModel';
import { styles } from './styles';

function RecordedState({ result }: { result: OutcomeResponse }) {
  return (
    <Panel>
      <View style={styles.empty}>
        <Text style={styles.heading}>Progress recorded</Text>
        <Notice tone="success">
          <Text>{`Recorded: ${result.event.event_type}`}</Text>
        </Notice>
        {result.overview_error && (
          <Notice>
            <Text>{result.overview_error}</Text>
          </Notice>
        )}
        <Text style={styles.copy}>Refresh to open the next session.</Text>
        <Button id={`study-next-${result.event_id}`} tone="primary" onClick={reloadPage}>
          Open next session
        </Button>
      </View>
    </Panel>
  );
}

function FallbackActions({ action }: { action: StudyAction }) {
  const id = actionIdentity(action);
  return (
    <View style={styles.actions}>
      <AppLink
        id={`study-open-file-${id}`}
        href={fallbackHref(action)}
        target="_blank"
        rel="noopener noreferrer"
        style={styles.link}
      >
        Open file
      </AppLink>
      <AppLink id={`study-open-course-${id}`} href={`/courses/${action.course_id}`} style={styles.link}>
        Course
      </AppLink>
      <Text style={styles.muted}>Current level</Text>
      <StudyLevel
        key={id}
        id={`study-current-level-${id}`}
        courseId={action.course_id}
        filePath={action.source_path || action.file_path}
        initial={action.level}
      />
    </View>
  );
}

function ActionContext({ action, fallback }: { action: StudyAction; fallback: boolean }) {
  return (
    <View style={styles.actions}>
      {!!(action.minutes || action.reading_minutes) && (
        <Text style={styles.muted}>{`${action.minutes || action.reading_minutes} min`}</Text>
      )}
      {action.days_to_exam != null && <Text style={styles.muted}>{`${action.days_to_exam} days to exam`}</Text>}
      {fallback && <Text style={styles.muted}>File suggestion</Text>}
    </View>
  );
}

export function PrimaryAction({ action, fallback }: { action: StudyAction; fallback: boolean }) {
  const [recorded, setRecorded] = useState<OutcomeResponse | null>(null);
  if (recorded) return <RecordedState result={recorded} />;
  return (
    <Panel id={`study-primary-${actionIdentity(action)}`}>
      <View style={styles.stack}>
        <View style={styles.row}>
          <AppLink
            id={`study-primary-course-${actionIdentity(action)}`}
            href={`/courses/${action.course_id}`}
            style={styles.link}
          >
            {action.course_name}
          </AppLink>
          <ActionContext action={action} fallback={fallback} />
        </View>
        <Text style={styles.primaryHeading}>{conciseInstruction(action)}</Text>
        <View style={styles.actions}>
          {fallback ? <FallbackActions action={action} /> : <RecordOutcome action={action} onRecorded={setRecorded} />}
          <StudyEvidence action={action} />
        </View>
      </View>
    </Panel>
  );
}
