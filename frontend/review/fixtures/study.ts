import { http, HttpResponse } from 'msw';
import { z } from 'zod/v4';
import type { PlannerRow, StudyAction, StudyPayload } from '@/lib/types';
import type { ReviewFixture } from '../scenario';

export const studyDocumentId = 'document-101-matrices';
export const studyAction: StudyAction = {
  action_id: 'action-101-matrix-recall',
  action_type: 'read',
  course_id: 101,
  course_name: 'Linear Algebra',
  short_name: 'MATH101',
  instruction: 'Read matrix multiplication and explain the order of operations',
  unit_title: 'Matrices and linear transformations',
  source_path: '/Courses/101/eclass/Lectures/Matrices.pdf',
  file_name: 'Matrices.pdf',
  minutes: 30,
  estimated_minutes: 30,
  days_to_exam: 8,
  scheduled_date: '2026-10-13',
  plan_revision: 'plan-101-synthetic-1',
  success_criterion: 'Explain why AB and BA can differ using a two-by-two example.',
  evidence_links: [
    {
      document_id: studyDocumentId,
      renderable: true,
      source_path: '/Courses/101/eclass/Lectures/Matrices.pdf',
      label: 'Matrices — synthetic lecture',
      page_number: 1,
    },
  ],
};

const rows: PlannerRow[] = [
  {
    course_id: 101,
    course_name: 'Linear Algebra',
    short_name: 'MATH101',
    enabled: true,
    exam_at: '2026-10-21T09:00',
    commitment: 'must_pass',
    target_grade: 8,
    planning_notes: 'Synthetic plan: review matrix products before transformations.',
  },
  {
    course_id: 202,
    course_name: 'Algorithms',
    short_name: 'CS202',
    enabled: true,
    exam_at: '2026-10-28T12:00',
    commitment: 'committed',
    target_grade: 7,
  },
  {
    course_id: 303,
    course_name: 'Statistics',
    short_name: 'STAT303',
    enabled: false,
    commitment: 'conditional',
    target_grade: 6,
  },
];

export function syntheticStudyPayload(state = 'ready'): StudyPayload {
  const empty = state === 'empty';
  const pending = state === 'pending';
  const alternative: StudyAction = {
    ...studyAction,
    action_id: 'action-202-complexity-recall',
    course_id: 202,
    course_name: 'Algorithms',
    short_name: 'CS202',
    instruction: 'Compare linear and binary search costs',
    minutes: 20,
    days_to_exam: 15,
    plan_revision: 'plan-202-synthetic-1',
    evidence_links: [],
    source_path: undefined,
    file_name: undefined,
  };
  return {
    generated_at: '2026-10-13T10:00:00Z',
    generation: 1,
    status: pending ? 'building' : 'ready',
    study_projection_status: pending ? 'pending' : 'ready',
    adaptive_plan: empty || pending ? {} : { next_session: studyAction, today_queue: [studyAction, alternative] },
    study_intelligence: { focus_queue: pending ? [studyAction] : [], coverage: { enriched: 1, total: 1 } },
    inbox: [],
    planner_rows: rows.map((row) => (empty ? { ...row, enabled: false, exam_at: undefined } : { ...row })),
    planner_settings: {
      weekly_minutes: { '0': 90, '1': 60, '2': 90, '3': 60, '4': 45, '5': 120, '6': 0 },
      block_minutes: 30,
      max_courses_per_day: 2,
      blackout_dates: ['2026-10-18'],
    },
  };
}

type StudyState = 'ready' | 'empty' | 'pending' | 'loading' | 'load-recovery' | 'save-recovery';

function applyPlanner(payload: StudyPayload, fields: Record<string, string>): void {
  payload.planner_rows = payload.planner_rows?.map((row) => ({
    ...row,
    enabled: fields[`enabled_${row.course_id}`] === 'on',
    short_name: fields[`short_name_${row.course_id}`],
    exam_at: fields[`exam_at_${row.course_id}`],
    target_grade: Number(fields[`target_grade_${row.course_id}`]),
    commitment: fields[`commitment_${row.course_id}`],
    planning_notes: fields[`planning_notes_${row.course_id}`],
  }));
  payload.planner_settings = {
    weekly_minutes: Object.fromEntries(
      Array.from({ length: 7 }, (_, day) => [String(day), Number(fields[`weekly_minutes_${day}`])]),
    ),
    block_minutes: Number(fields.block_minutes),
    max_courses_per_day: Number(fields.max_courses_per_day),
    blackout_dates: (fields.blackout_dates || '').split(/[\s,]+/).filter(Boolean),
  };
}

export function studyFixture(state: StudyState = 'ready'): ReviewFixture & { release: () => void } {
  let release = () => {};
  return {
    id: `synthetic-study-${state}-v1`,
    release: () => release(),
    handlers: () => {
      const payload = syntheticStudyPayload(state);
      const pending = new Promise<void>((resolve) => {
        release = resolve;
      });
      let reads = 0;
      let saves = 0;
      return [
        http.get('/api/v1/study', async () => {
          if (state === 'loading') await pending;
          if (state === 'load-recovery' && reads++ === 0) {
            return HttpResponse.json(
              { detail: 'Synthetic study service is temporarily unavailable.' },
              { status: 503 },
            );
          }
          return HttpResponse.json(payload);
        }),
        http.post('/api/v1/study/planner', async ({ request }) => {
          if (state === 'save-recovery' && saves++ === 0) {
            return HttpResponse.json(
              { detail: 'Synthetic planner save failed. Your edits are still here.' },
              { status: 503 },
            );
          }
          applyPlanner(payload, z.record(z.string(), z.string()).parse(await request.json()));
          return HttpResponse.json({ ok: true });
        }),
        http.post('/api/v1/study/actions/event', () => HttpResponse.json({ ok: true })),
      ];
    },
  };
}
