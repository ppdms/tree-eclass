import type { PracticeQuestion, PracticeUnit, PracticeView } from '@/features/session/reader/types';

export type PracticeOutcome = 'correct' | 'partial' | 'incorrect' | 'skipped';

export interface CoursePracticeQuestion extends PracticeQuestion {
  blueprint_revision_id?: string;
  set_hash?: string;
  last_outcome?: PracticeOutcome;
  last_attempted_at?: string;
}

export interface CoursePracticeUnit extends Omit<PracticeUnit, 'questions'> {
  unit_key?: string;
  blueprint_revision_id?: string;
  questions?: CoursePracticeQuestion[];
}

export interface CoursePracticeView extends Omit<PracticeView, 'units' | 'totals'> {
  course_id?: string | number;
  blueprint_revision_id?: string | null;
  units?: CoursePracticeUnit[];
  totals?: { due?: number; learned?: number; questions?: number; attempted?: number; units?: number };
}

export interface PracticeAttemptBody {
  course_id: string | number;
  question_id: string;
  outcome: PracticeOutcome;
  idempotency_key: string;
  seconds?: number;
  note?: string;
}

export interface RecordedPracticeAttempt {
  status: 'recorded';
  attempt: { id?: string | number; unit_key?: string; question_id?: string; outcome?: PracticeOutcome };
  practice: CoursePracticeView | null;
  practice_error?: string | null;
}

export function practiceQuestionIdentity(question: CoursePracticeQuestion | undefined): string {
  return question
    ? JSON.stringify([
        question.blueprint_revision_id,
        question.unit_key,
        question.set_hash,
        question.question_id || question.id,
      ])
    : '';
}

export function matchingPracticeView(
  view: CoursePracticeView | null,
  courseId: string | number,
  revision: string | undefined,
): boolean {
  return Boolean(
    view?.enabled === true &&
    String(view.course_id) === String(courseId) &&
    revision &&
    view.blueprint_revision_id === revision,
  );
}

export function mergePracticeUnit(
  current: CoursePracticeView,
  refreshed: CoursePracticeView,
  unitKey: string | undefined,
): CoursePracticeView {
  if (!unitKey || !refreshed.blueprint_revision_id || current.blueprint_revision_id !== refreshed.blueprint_revision_id)
    return refreshed;
  const updates = refreshed.units || [];
  const units = (current.units || []).flatMap((unit) => {
    if (unit.unit_key !== unitKey) return [unit];
    return updates.filter((replacement) => replacement.unit_key === unitKey);
  });
  for (const unit of updates) {
    if (!units.some((present) => present.unit_key === unit.unit_key)) units.push(unit);
  }
  const questions = units.flatMap((unit) => unit.questions || []);
  return {
    ...current,
    ...refreshed,
    units,
    totals: {
      units: units.length,
      questions: questions.length,
      due: questions.filter((question) => ['needs_work', 'unattempted'].includes(question.state || '')).length,
      learned: questions.filter((question) => question.state === 'learned').length,
      attempted: questions.filter((question) => Number(question.attempts || 0) > 0).length,
    },
  };
}
