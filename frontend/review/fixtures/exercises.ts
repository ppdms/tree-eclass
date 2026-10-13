import { http, HttpResponse } from 'msw';
import type { Exercise, ExercisesPayload } from '@/lib/types';

const summaries: Exercise[] = [
  {
    course_id: 101,
    exercise_id: 'matrix-problems',
    title: 'Matrix systems problem set',
    course_name: 'Linear Algebra',
    submission_status: 'not submitted',
    _urgency: 'ex-overdue',
    _time_label: 'Overdue · 2 days',
    deadline: '2026-10-11T17:00:00Z',
    fetched_at: '2026-10-13T09:45:00Z',
    link: 'https://example.invalid/courses/101/assignments/matrix-problems',
  },
  {
    course_id: 202,
    exercise_id: 'algorithm-analysis',
    title: 'Algorithm complexity analysis',
    course_name: 'Algorithms',
    submission_status: 'not submitted',
    _urgency: 'ex-upcoming',
    _time_label: 'Due in 3 days',
    deadline: '2026-10-16T14:00:00Z',
    fetched_at: '2026-10-13T09:45:00Z',
    link: 'https://example.invalid/courses/202/assignments/algorithm-analysis',
  },
  {
    course_id: 303,
    exercise_id: 'probability-report',
    title: 'Probability simulation report',
    course_name: 'Statistics',
    submission_status: 'submitted',
    grade: '8.5 / 10',
    _time_label: 'Due 9 Oct, 17:00',
    deadline: '2026-10-09T14:00:00Z',
    fetched_at: '2026-10-13T09:45:00Z',
    link: 'https://example.invalid/courses/303/assignments/probability-report',
  },
  {
    course_id: 101,
    exercise_id: 'optional-vectors',
    title: 'Optional vector warm-up',
    ignored: true,
    course_name: 'Linear Algebra',
    submission_status: 'not submitted',
    _time_label: 'No deadline',
    fetched_at: '2026-10-13T09:45:00Z',
    link: 'https://example.invalid/courses/101/assignments/optional-vectors',
  },
];

function detail(exercise: Exercise): Exercise {
  if (exercise.exercise_id === 'probability-report') {
    return {
      ...exercise,
      description: '<p>Simulate independent trials and compare empirical probabilities.</p>',
      work_type: 'Individual report',
      grade_comments: 'Clear plots and sound interpretation; explain the seed choice.',
      submission_date: '9 Oct 2026, 12:42',
    };
  }
  return {
    ...exercise,
    work_type: 'Individual problem set',
    description:
      '<p>Solve the three linear systems using row reduction.</p>' +
      '<ol><li>Show each elementary operation.</li><li>State rank and describe all solutions.</li></ol>',
  };
}

export function exercisesFixture(state: 'normal' | 'empty' | 'detail-recovery' | 'hide-recovery' = 'normal') {
  let releaseDetails = () => {};
  return {
    id: `synthetic-exercises-${state}-v1`,
    releaseDetails: () => releaseDetails(),
    handlers: () => {
      let detailRequests = 0;
      let hideRequests = 0;
      const pending = new Promise<void>((resolve) => {
        releaseDetails = resolve;
      });
      return [
        http.get('/api/v1/exercises', () =>
          HttpResponse.json({
            exercises: state === 'empty' ? [] : summaries,
          } satisfies ExercisesPayload),
        ),
        http.get('/api/v1/courses/:courseId/exercises/:exerciseId', async ({ params }) => {
          const exercise = summaries.find(
            (item) => String(item.course_id) === params.courseId && item.exercise_id === params.exerciseId,
          );
          if (!exercise) return HttpResponse.json({ detail: 'Exercise not found' }, { status: 404 });
          if (state === 'detail-recovery' && ++detailRequests === 1) {
            await pending;
            return HttpResponse.json({ detail: 'Synthetic exercise detail unavailable' }, { status: 503 });
          }
          return HttpResponse.json({ exercise: detail(exercise) });
        }),
        http.post('/api/v1/courses/:courseId/exercises/:exerciseId/ignore', () => {
          if (state === 'hide-recovery' && ++hideRequests === 1) {
            return HttpResponse.json({ detail: 'Synthetic hide operation was not saved.' }, { status: 503 });
          }
          return HttpResponse.json({ status: 'ok' });
        }),
        http.post('/api/v1/courses/:courseId/exercises/:exerciseId/unignore', () =>
          HttpResponse.json({ status: 'ok' }),
        ),
      ];
    },
  };
}
