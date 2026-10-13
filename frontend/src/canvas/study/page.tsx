import { useState } from 'react';
import { useSearchParams } from 'react-router';
import type { StudyPayload } from '@/lib/types';
import { AppLink } from '../app/links';
import { Button, Notice, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { Alternatives } from './alternatives';
import { ExamHorizon } from './calendar';
import { studySelection } from './model';
import { Planner } from './planner';
import { PrimaryAction } from './primary';
import { styles } from './styles';
import { RunCheckButton } from './support';
import { useStudyPlan } from './useStudyPlan';

function EmptyStudy({ onAddDates }: { onAddDates: () => void }) {
  return (
    <Panel id="study-empty">
      <View style={styles.empty}>
        <Text style={styles.heading}>No next action is ready</Text>
        <Text style={styles.copy}>Add exam dates or refresh courses to build a plan.</Text>
        <View style={styles.actions}>
          <RunCheckButton />
          <Button id="study-empty-add-dates" onClick={onAddDates}>
            Add exam dates
          </Button>
        </View>
      </View>
    </Panel>
  );
}

function StudyError({ error, loading, onRetry }: { error: string; loading: boolean; onRetry: () => void }) {
  return (
    <View style={styles.page}>
      <Panel id="study-load-error">
        <View style={styles.empty}>
          <Text style={styles.heading}>Could not load this study plan</Text>
          <Notice tone="danger">
            <Text>{error}</Text>
          </Notice>
          <View style={styles.actions}>
            <Button id="study-retry" disabled={loading} tone="primary" onClick={onRetry}>
              {loading ? 'Opening study planner…' : 'Try again'}
            </Button>
            <AppLink id="study-open-all" href="/study" style={styles.link}>
              Open all courses
            </AppLink>
          </View>
        </View>
      </Panel>
    </View>
  );
}

export function StudyPage({ initialData }: { initialData: StudyPayload }) {
  const [search] = useSearchParams();
  const [plannerOpen, setPlannerOpen] = useState(false);
  const { data, error, loading, load } = useStudyPlan(search.get('course_id'), initialData);
  const { primary, alternatives, fallback } = studySelection(data);
  const refresh = () => {
    void load();
  };
  if (error) return <StudyError error={error} loading={loading} onRetry={refresh} />;
  return (
    <View id="study-page" ready style={styles.page}>
      <Text style={styles.title}>Study</Text>
      {data.study_projection_status === 'pending' && (
        <Notice id="study-projection-pending">
          <Text>The study plan is being prepared. Available course updates are shown below.</Text>
        </Notice>
      )}
      {loading && (
        <Notice id="study-refresh-pending">
          <Text>Opening study planner…</Text>
        </Notice>
      )}
      <View style={styles.dashboard}>
        <View style={styles.stack}>
          {primary ? (
            <PrimaryAction action={primary} fallback={fallback} />
          ) : (
            <EmptyStudy onAddDates={() => setPlannerOpen(true)} />
          )}
          <Alternatives actions={alternatives} fallback={fallback} />
        </View>
        <ExamHorizon rows={data.planner_rows || []} onEdit={() => setPlannerOpen(true)} />
      </View>
      <Planner data={data} open={plannerOpen} onClose={() => setPlannerOpen(false)} onSaved={refresh} />
    </View>
  );
}
