import type { PlannerRow } from '@/lib/types';
import { daysUntilCalendarDate, formatCalendarDate, formatTimeOfDay, parseTimestamp } from '@/lib/format';
import { AppLink } from '../app/links';
import { Button, Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { styles } from './styles';

export interface ExamRow extends PlannerRow {
  date: Date;
  daysLeft: number;
}

export function examRows(rows: PlannerRow[], now: Date = new Date()): ExamRow[] {
  const exams: ExamRow[] = [];
  for (const row of rows) {
    if (!row.enabled || !row.exam_at) continue;
    const date = parseTimestamp(row.exam_at);
    if (!date) continue;
    const daysLeft = daysUntilCalendarDate(date, now);
    if (daysLeft >= 0) exams.push({ ...row, date, daysLeft });
  }
  return exams.sort((a, b) => a.date.getTime() - b.date.getTime());
}

function ExamCountdown({ exam }: { exam: ExamRow }) {
  return (
    <View style={styles.section}>
      <Text style={styles.label}>Next exam</Text>
      <AppLink id={`study-next-exam-${exam.course_id}`} href={`/courses/${exam.course_id}`} style={styles.link}>
        {exam.course_name}
      </AppLink>
      <Text style={styles.muted}>{`${formatCalendarDate(exam.date)} · ${formatTimeOfDay(exam.date)}`}</Text>
      <View style={styles.actions}>
        <Text label={exam.daysLeft > 0 ? `${exam.daysLeft} days until exam` : 'Exam is today'} style={styles.countdown}>
          {exam.daysLeft ? String(exam.daysLeft) : 'Now'}
        </Text>
        {exam.daysLeft > 0 && <Text style={styles.muted}>{exam.daysLeft === 1 ? 'day' : 'days'}</Text>}
      </View>
    </View>
  );
}

function ExamTimelineRow({ exam }: { exam: ExamRow }) {
  return (
    <AppLink id={`study-exam-${exam.course_id}`} block href={`/courses/${exam.course_id}`} style={styles.examRow}>
      <Text style={styles.copy}>{exam.short_name || exam.course_name}</Text>
      <Text style={styles.muted}>{formatCalendarDate(exam.date)}</Text>
      <Text style={styles.label}>{`${exam.daysLeft}d`}</Text>
    </AppLink>
  );
}

export function ExamHorizon({ rows, onEdit }: { rows: PlannerRow[]; onEdit: () => void }) {
  const [next, ...later] = examRows(rows);
  return (
    <Panel id="study-exam-horizon">
      <View style={styles.stack}>
        <View style={styles.row}>
          <Text style={styles.heading}>Exams</Text>
          {next && (
            <Button id="study-edit-exams" onClick={onEdit}>
              Edit
            </Button>
          )}
        </View>
        {next ? (
          <>
            <ExamCountdown exam={next} />
            {later.map((exam) => (
              <ExamTimelineRow key={String(exam.course_id)} exam={exam} />
            ))}
          </>
        ) : (
          <View style={styles.empty}>
            <Text style={styles.copy}>No exams planned</Text>
            <Button id="study-add-dates" onClick={onEdit}>
              Add dates
            </Button>
          </View>
        )}
      </View>
    </Panel>
  );
}
