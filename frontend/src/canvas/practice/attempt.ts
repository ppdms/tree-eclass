import { intentKey } from '../roadmap/styles';
import type { CoursePracticeQuestion, PracticeAttemptBody, PracticeOutcome } from './model';

export function buildCoursePracticeAttempt(
  courseId: string | number,
  question: CoursePracticeQuestion,
  revealed: boolean,
  note: string,
  outcome: PracticeOutcome,
): PracticeAttemptBody {
  const body: PracticeAttemptBody = {
    course_id: courseId,
    question_id: String(question.question_id || question.id || ''),
    outcome,
    idempotency_key: intentKey('practice'),
  };
  if (revealed) body.seconds = 5;
  if (note.trim()) body.note = note.trim();
  return body;
}
