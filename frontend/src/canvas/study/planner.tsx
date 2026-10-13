import { useState } from 'react';
import type { StudyPayload } from '@/lib/types';
import { useViewport } from '../app/viewport';
import { Button, Notice } from '../ui/controls';
import { Dialog, ScrollArea } from '../ui/overlays';
import { Text, View } from '../ui/primitives';
import { PlannerAvailability } from './plannerAvailability';
import { PlannerCourse } from './plannerCourse';
import { createPlannerDraft, type PlannerDraft } from './plannerModel';
import { styles } from './styles';
import { usePlanner, type PlannerState } from './usePlanner';

function PlannerMessages({ state, errors }: { state: string | null; errors: string[] }) {
  if (errors.length)
    return (
      <Notice id="study-planner-errors" tone="danger">
        <View style={styles.stack}>
          <Text style={styles.label}>Check these fields:</Text>
          {errors.map((message) => (
            <Text key={message} style={styles.copy}>{`• ${message}`}</Text>
          ))}
        </View>
      </Notice>
    );
  return state ? (
    <Notice id="study-planner-state" tone="success">
      <Text>{state}</Text>
    </Notice>
  ) : null;
}

function PlannerCourses({ draft, onChange }: { draft: PlannerDraft; onChange: (draft: PlannerDraft) => void }) {
  return (
    <View style={styles.stack}>
      {draft.courses.map((course) => (
        <PlannerCourse
          key={String(course.row.course_id)}
          course={course}
          onChange={(next) =>
            onChange({
              ...draft,
              courses: draft.courses.map((item) => (item.row.course_id === next.row.course_id ? next : item)),
            })
          }
        />
      ))}
    </View>
  );
}

interface FormProps {
  data: StudyPayload;
  onClose: () => void;
  planner: PlannerState;
}

type PlannerTab = 'exams' | 'availability';

function PlannerTabs({ tab, onTab }: { tab: PlannerTab; onTab: (tab: PlannerTab) => void }) {
  return (
    <View style={styles.actions}>
      <Button
        id="study-planner-tab-exams"
        pressed={tab === 'exams'}
        controls="study-planner-exams"
        style={tab === 'exams' ? styles.selectedTab : undefined}
        onClick={() => onTab('exams')}
      >
        Exams
      </Button>
      <Button
        id="study-planner-tab-availability"
        pressed={tab === 'availability'}
        controls="study-planner-availability"
        style={tab === 'availability' ? styles.selectedTab : undefined}
        onClick={() => onTab('availability')}
      >
        Availability
      </Button>
    </View>
  );
}

function PlannerForm({ data, onClose, planner }: FormProps) {
  const [draft, setDraft] = useState(() => createPlannerDraft(data));
  const [tab, setTab] = useState<PlannerTab>('exams');
  const viewport = useViewport();
  return (
    <Dialog
      id="study-planner"
      title="Study plan"
      open
      onClose={onClose}
      actions={
        <>
          <Button id="study-planner-close" onClick={onClose}>
            Close
          </Button>
          <Button
            id="study-planner-save"
            tone="primary"
            disabled={planner.saving}
            onClick={() => {
              void planner.save(draft);
            }}
          >
            {planner.saving ? 'Saving…' : 'Save plan'}
          </Button>
        </>
      }
    >
      <View style={styles.stack}>
        <PlannerMessages state={planner.state} errors={planner.errors} />
        <PlannerTabs tab={tab} onTab={setTab} />
        <ScrollArea id="study-planner-scroll" height={Math.max(180, viewport.height - 240)}>
          <View id="study-planner-exams" style={{ display: tab === 'exams' ? 'flex' : 'none' }}>
            <PlannerCourses draft={draft} onChange={setDraft} />
          </View>
          <View id="study-planner-availability" style={{ display: tab === 'availability' ? 'flex' : 'none' }}>
            <PlannerAvailability draft={draft} onChange={setDraft} />
          </View>
        </ScrollArea>
      </View>
    </Dialog>
  );
}

export function Planner({
  data,
  open,
  onClose,
  onSaved,
}: {
  data: StudyPayload;
  open: boolean;
  onClose: () => void;
  onSaved: () => void;
}) {
  const planner = usePlanner(onSaved, onClose);
  return open ? <PlannerForm data={data} onClose={onClose} planner={planner} /> : null;
}
