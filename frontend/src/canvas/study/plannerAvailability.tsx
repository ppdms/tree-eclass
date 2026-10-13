import { Field } from '../ui/forms';
import { Text, View } from '../ui/primitives';
import { DAYS, type PlannerDraft } from './plannerModel';
import { styles } from './styles';

function WeekGrid({ draft, onChange }: { draft: PlannerDraft; onChange: (draft: PlannerDraft) => void }) {
  return (
    <View style={styles.weekGrid}>
      {DAYS.map((day, index) => (
        <View key={day} style={styles.stack}>
          <Field
            id={`weekly_minutes_${index}`}
            label={day.slice(0, 3)}
            type="number"
            min={0}
            max={1440}
            step={1}
            value={draft.weekly[index] || ''}
            onChange={(minutes) => {
              const weekly = [...draft.weekly];
              weekly[index] = minutes;
              onChange({ ...draft, weekly });
            }}
          />
          <Text style={styles.muted}>min</Text>
        </View>
      ))}
    </View>
  );
}

export function PlannerAvailability({
  draft,
  onChange,
}: {
  draft: PlannerDraft;
  onChange: (draft: PlannerDraft) => void;
}) {
  return (
    <View style={styles.stack}>
      <Text style={styles.heading}>Weekly availability</Text>
      <WeekGrid draft={draft} onChange={onChange} />
      <View style={styles.fieldGrid}>
        <Field
          id="block_minutes"
          label="Session minutes"
          type="number"
          min={15}
          max={1440}
          step={1}
          value={draft.blockMinutes}
          onChange={(blockMinutes) => onChange({ ...draft, blockMinutes })}
        />
        <Field
          id="max_courses_per_day"
          label="Courses per day"
          type="number"
          min={1}
          max={7}
          step={1}
          value={draft.maxCourses}
          onChange={(maxCourses) => onChange({ ...draft, maxCourses })}
        />
      </View>
      <Field
        id="blackout_dates"
        label="Days off"
        value={draft.blackoutDates}
        placeholder="YYYY-MM-DD, YYYY-MM-DD"
        onChange={(blackoutDates) => onChange({ ...draft, blackoutDates })}
      />
    </View>
  );
}
