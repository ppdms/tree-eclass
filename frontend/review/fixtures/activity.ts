import { http, HttpResponse } from 'msw';
import type { ActivityGroup, InboxPayload } from '@/lib/types';
import type { ReviewFixture } from '../scenario';

const groups: ActivityGroup[] = [
  {
    id: 'synthetic-matrices-update',
    title: 'Matrices lecture revised',
    type: 'change',
    importance: 'informational',
    timestamp: '2026-10-13T08:30:00Z',
    items: [
      {
        type: 'change',
        course_id: 101,
        course_short_name: 'MATH101',
        change_no: 'revision-2',
        timestamp: '2026-10-13T08:30:00Z',
        changes: [
          {
            change_type: 'modified_file',
            file_path: '/Courses/101/eclass/Lectures/Matrices.pdf',
            name: 'Matrices.pdf',
          },
          {
            change_type: 'newfile',
            file_path: '/Courses/101/eclass/Practice/Row-reduction.pdf',
            name: 'Row-reduction.pdf',
          },
          {
            change_type: 'deleted_file',
            file_path: '/Courses/101/eclass/Lectures/Old-handout.pdf',
            name: 'Old-handout.pdf',
          },
        ],
      },
    ],
  },
  {
    id: 'synthetic-exam-announcement',
    title: 'Midterm room and submission deadline',
    type: 'announcement',
    importance: 'important',
    items: [
      {
        course_id: 202,
        course_short_name: 'CS202',
        timestamp: '2026-10-13T07:15:00Z',
        link: 'https://example.invalid/courses/202/announcements/midterm',
        description: '<p>The midterm is in room B12.</p><p>Submit your algorithm analysis by Friday at 17:00.</p>',
      },
    ],
  },
  {
    id: 'synthetic-office-hours',
    title: 'Statistics office hours moved',
    type: 'announcement',
    importance: 'informational',
    items: [
      {
        course_id: 303,
        course_name: 'Statistics',
        timestamp: '2026-10-12T11:00:00Z',
        link: 'https://example.invalid/courses/303/announcements/hours',
        description: 'Office hours now take place on Wednesday, 14:00–16:00, in room C4.',
      },
    ],
  },
];

export function activityFixture(empty = false): ReviewFixture {
  return {
    id: `synthetic-activity-${empty ? 'empty' : 'triage'}-v1`,
    handlers: () => [
      http.get('/api/v1/inbox', () =>
        HttpResponse.json({
          groups: empty ? [] : groups,
          next_offset: empty ? 0 : 3,
          has_more: false,
        } satisfies InboxPayload),
      ),
    ],
  };
}

export function olderUpdatesFixture() {
  let releaseOlder = () => {};
  return {
    id: 'synthetic-activity-older-recovery-v1',
    releaseOlder: () => releaseOlder(),
    handlers: () => {
      let requests = 0;
      const pending = new Promise<void>((resolve) => {
        releaseOlder = resolve;
      });
      return [
        http.get('/api/v1/inbox', async ({ request }) => {
          if (new URL(request.url).searchParams.get('offset') === '0') {
            return HttpResponse.json({ groups, next_offset: 3, has_more: true } satisfies InboxPayload);
          }
          if (++requests === 1) {
            await pending;
            return HttpResponse.json({ detail: 'Synthetic older-updates service unavailable' }, { status: 503 });
          }
          return HttpResponse.json({
            groups: [
              {
                id: 'synthetic-older-update',
                type: 'announcement',
                title: 'Reading list for probability distributions',
                timestamp: '2026-10-11T08:00:00Z',
                items: [
                  {
                    course_id: 303,
                    course_name: 'Statistics',
                    description: 'Read chapters 3 and 4 before the next tutorial.',
                    link: 'https://example.invalid/courses/303/announcements/reading',
                  },
                ],
              },
            ],
            next_offset: 4,
            has_more: false,
          } satisfies InboxPayload);
        }),
      ];
    },
  };
}
