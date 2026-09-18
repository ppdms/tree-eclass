import * as stylex from '@stylexjs/stylex';
import { plannerStyles } from './plannerStyles';
import * as React from 'react';
import { z } from 'zod/v4';
import { CalendarRange, Clock3, Save } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import type { PlannerRow, PlannerSettings, StudyPayload } from '@/lib/types';
import { PlannerCourseCard } from './plannerCells';
import { PlannerSheet } from './plannerSheet';

const styles = plannerStyles;

export const DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];

export function PlannerMessages({ saveState, errors }: { saveState: string | null; errors: string[] }) {
  if (errors.length)
    return (
      <div {...stylex.props(styles.plannerMessage, styles.plannerMessageWarn)} id="planner-errors" role="alert">
        <strong>Check these fields:</strong>
        <ul>
          {errors.map((message, index) => (
            <li key={`${message}-${index}`}>{message}</li>
          ))}
        </ul>
      </div>
    );
  return (
    saveState && (
      <div {...stylex.props(styles.plannerMessage, styles.plannerMessageOk)} role="status">
        {saveState}
      </div>
    )
  );
}

export function WeekGrid({ settings }: { settings: PlannerSettings }) {
  return (
    <div {...stylex.props(styles.studyPlanWeekGrid)}>
      {DAYS.map((day, index) => (
        <label key={day} {...stylex.props(styles.weekGridLabel)}>
          <span>{day.slice(0, 3)}</span>
          <Input
            type="number"
            name={`weekly_minutes_${index}`}
            min="0"
            max="1440"
            defaultValue={settings.weekly_minutes?.[String(index)] || 0}
            style={styles.weekGridInput}
          />
          <small {...stylex.props(styles.weekGridUnit)}>min</small>
        </label>
      ))}
    </div>
  );
}

export function ControlGrid({ settings }: { settings: PlannerSettings }) {
  return (
    <div {...stylex.props(styles.studyPlanControlGrid)}>
      <label {...stylex.props(styles.controlGridLabel)}>
        <span>Session minutes</span>
        <Input type="number" name="block_minutes" min="15" max="1440" defaultValue={settings.block_minutes || 45} />
      </label>
      <label {...stylex.props(styles.controlGridLabel)}>
        <span>Courses per day</span>
        <Input
          type="number"
          name="max_courses_per_day"
          min="1"
          max="7"
          defaultValue={settings.max_courses_per_day || 3}
        />
      </label>
      <label {...stylex.props(styles.controlGridLabel, styles.studyPlanBlackout)}>
        <span>Days off</span>
        <Input
          type="text"
          name="blackout_dates"
          defaultValue={(settings.blackout_dates || []).join(', ')}
          placeholder="YYYY-MM-DD, YYYY-MM-DD"
        />
      </label>
    </div>
  );
}

export function WeeklyCapacity({ settings }: { settings: PlannerSettings }) {
  return (
    <fieldset {...stylex.props(styles.studyPlanCapacity)}>
      <legend {...stylex.props(styles.studyPlanLegend)}>Weekly availability</legend>
      <WeekGrid settings={settings} />
      <ControlGrid settings={settings} />
    </fieldset>
  );
}

function PlannerCourses({ rows }: { rows: PlannerRow[] }) {
  return (
    <div {...stylex.props(styles.studyPlanCourseList)}>
      {rows.map((row) => (
        <PlannerCourseCard row={row} key={row.course_id} />
      ))}
    </div>
  );
}

export function PlannerForm({
  rows,
  settings,
  onSubmit,
  saving,
  saveState,
  errors,
}: {
  rows: PlannerRow[];
  settings: PlannerSettings;
  onSubmit: (event: React.FormEvent<HTMLFormElement>) => void;
  saving: boolean;
  saveState: string | null;
  errors: string[];
}) {
  return (
    <form
      method="POST"
      action="/study/planner"
      onSubmit={onSubmit}
      {...stylex.props(styles.studyPlannerForm)}
      aria-describedby="planner-errors"
    >
      <PlannerFormBody rows={rows} settings={settings} saveState={saveState} errors={errors} />
      <footer {...stylex.props(styles.studyPlanSheetFooter)}>
        <Button type="submit" disabled={saving} icon={<Save aria-hidden="true" />}>
          {saving ? 'Saving…' : 'Save plan'}
        </Button>
      </footer>
    </form>
  );
}

function PlannerFormBody({
  rows,
  settings,
  saveState,
  errors,
}: Pick<Parameters<typeof PlannerForm>[0], 'rows' | 'settings' | 'saveState' | 'errors'>) {
  return (
    <div {...stylex.props(styles.studyPlanSheetBody)}>
      <PlannerMessages saveState={saveState} errors={errors} />
      <Tabs defaultValue="exams">
        <TabsList {...stylex.props(styles.studyPlanTabs)}>
          <TabsTrigger value="exams">
            <CalendarRange aria-hidden="true" /> Exams
          </TabsTrigger>
          <TabsTrigger value="availability">
            <Clock3 aria-hidden="true" /> Availability
          </TabsTrigger>
        </TabsList>
        {/* Keep both panels mounted so FormData includes settings from both tabs. TabsContent owns visibility. */}
        <TabsContent value="exams" forceMount>
          <PlannerCourses rows={rows} />
        </TabsContent>
        <TabsContent value="availability" forceMount>
          <WeeklyCapacity settings={settings} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

function validatePlannerForm(form: HTMLFormElement): string[] {
  const errors: string[] = [];
  const fields = new FormData(form);
  const checkInteger = (name: string, label: string, min: number, max: number) => {
    const value = String(fields.get(name) ?? '').trim();
    if (!/^\d+$/.test(value) || Number(value) < min || Number(value) > max) {
      errors.push(`${label} must be a whole number between ${min} and ${max}.`);
    }
  };
  DAYS.forEach((day, index) => checkInteger(`weekly_minutes_${index}`, `${day} minutes`, 0, 1440));
  checkInteger('block_minutes', 'Session length', 15, 1440);
  checkInteger('max_courses_per_day', 'Maximum courses per day', 1, 7);
  const blackoutDates = String(fields.get('blackout_dates') || '')
    .trim()
    .split(/[\s,]+/)
    .filter(Boolean);
  blackoutDates.forEach((value) => {
    const date = new Date(`${value}T00:00:00Z`);
    if (
      !/^\d{4}-\d{2}-\d{2}$/.test(value) ||
      Number.isNaN(date.getTime()) ||
      date.toISOString().slice(0, 10) !== value
    ) {
      errors.push(`Blackout date “${value}” must use YYYY-MM-DD.`);
    }
  });
  form.querySelectorAll<HTMLInputElement>('input[name^="enabled_"]:checked').forEach((enabled) => {
    const suffix = enabled.name.slice('enabled_'.length);
    // SAFETY: the named item is an input rendered by the planner form; the
    // cast narrows the generic RadioNodeList/Element to read its value.
    const exam = form.elements.namedItem(`exam_at_${suffix}`) as HTMLInputElement | null;
    if (!exam?.value) errors.push('Enabled courses need an exam date.');
  });
  form.querySelectorAll<HTMLInputElement>('input[name^="target_grade_"]').forEach((input) => {
    const value = Number(input.value);
    if (input.value && (!Number.isFinite(value) || value < 0 || value > 10))
      errors.push('Target grades must be between 0 and 10.');
  });
  form.querySelectorAll<HTMLTextAreaElement>('textarea[name^="planning_notes_"]').forEach((input) => {
    if (input.value.length > 2000) errors.push('Planning notes must be at most 2000 characters.');
  });
  return [...new Set(errors)];
}

const errorBodySchema = z
  .object({
    detail: z.unknown(),
  })
  .partial();

const detailItemSchema = z
  .object({
    msg: z.string(),
  })
  .partial();

async function submitPlannerForm(form: HTMLFormElement): Promise<{ ok: true } | { ok: false; messages: string[] }> {
  try {
    const response = await fetch('/study/planner', {
      method: 'POST',
      body: new FormData(form),
      headers: { Accept: 'application/json' },
    });
    const parsed = errorBodySchema.safeParse(await response.json().catch(() => ({})));
    if (!response.ok) {
      const detail = parsed.success ? parsed.data.detail : undefined;
      const messages = Array.isArray(detail)
        ? detail.map((item) => {
            const parsedItem = detailItemSchema.safeParse(item);
            return parsedItem.success && parsedItem.data.msg ? parsedItem.data.msg : String(item);
          })
        : [String(detail || `Could not save study plan (HTTP ${response.status}).`)];
      return { ok: false, messages };
    }
    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      messages: [
        error instanceof Error ? error.message : 'Could not save study plan. Check your connection and try again.',
      ],
    };
  }
}

function usePlanner(onSaved?: () => void | Promise<void>) {
  const [saveState, setSaveState] = React.useState<string | null>(null);
  const [errors, setErrors] = React.useState<string[]>([]);
  React.useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    setSaveState(params.get('planner_saved') === '1' ? 'Study plan updated.' : params.get('planner_error') || null);
  }, []);
  const saving = saveState === 'Saving study plan…';
  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const clientErrors = validatePlannerForm(event.currentTarget);
    if (clientErrors.length) {
      setErrors(clientErrors);
      setSaveState(null);
      return;
    }
    setSaveState('Saving study plan…');
    setErrors([]);
    const result = await submitPlannerForm(event.currentTarget);
    if (!result.ok) {
      setErrors(result.messages);
      setSaveState(null);
      return;
    }
    setSaveState('Study plan saved.');
    await onSaved?.();
  };
  return { saveState, errors, saving, submit };
}

interface PlannerProps {
  data: StudyPayload;
  onSaved?: () => void | Promise<void>;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function Planner({ data, onSaved, open, onOpenChange }: PlannerProps) {
  const rows: PlannerRow[] = data.planner_rows || [];
  const settings: PlannerSettings = data.planner_settings || {};
  const afterSave = async () => {
    await onSaved?.();
    onOpenChange(false);
  };
  const { saveState, errors, saving, submit } = usePlanner(afterSave);
  return (
    <PlannerSheet open={open} onClose={() => onOpenChange(false)}>
      <PlannerForm
        rows={rows}
        settings={settings}
        onSubmit={submit}
        saving={saving}
        saveState={saveState}
        errors={errors}
      />
    </PlannerSheet>
  );
}
