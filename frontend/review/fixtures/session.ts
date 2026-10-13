import { http, HttpResponse } from 'msw';
import { z } from 'zod/v4';
import type { ReviewFixture } from '../scenario';
import type {
  Annotation,
  PagesPayload,
  PracticeView,
  SessionContext,
  SessionInfo,
} from '@/features/session/reader/types';
import { studyAction, studyDocumentId, studyFixture } from './study';

const annotationCreateSchema = z.object({
  kind: z.enum(['bookmark', 'highlight', 'note']),
  page_number: z.number(),
  body: z.string().nullable().optional(),
  quote: z.string().optional(),
  color: z.string().optional(),
  char_start: z.number().optional(),
  char_end: z.number().optional(),
  prefix: z.string().optional(),
  suffix: z.string().optional(),
  rects: z.array(z.object({ x: z.number(), y: z.number(), w: z.number(), h: z.number() })).optional(),
});
const annotationUpdateSchema = z.object({
  status: z.enum(['active', 'deleted', 'orphaned']).optional(),
  body: z.string().nullable().optional(),
  color: z.string().optional(),
});

export const sessionPath =
  '/study/session?course_id=101&action_id=action-101-matrix-recall' + '&document_id=document-101-matrices';
export const sessionContentPath = `/api/study/document/${studyDocumentId}/content`;
export const sessionInverseStatement = 'An invertible matrix has independent columns and a nonzero determinant.';
export const alternateDocumentId = 'document-101-row-reduction';
export const alternateContentPath = `/api/study/document/${alternateDocumentId}/content`;
export const alternateDocumentName = 'Row reduction — synthetic practice';
export const alternatePdfTitle = 'Synthetic Row Reduction Practice';

// Authored, ASCII-only one-page PDF with a real cross-reference table; no external source bytes.
function syntheticPdf(alternate = false, busy = false): Uint8Array<ArrayBuffer> {
  const text = alternate
    ? `BT /F1 20 Tf 50 720 Td (${alternatePdfTitle}) Tj ` +
      '0 -40 Td /F1 14 Tf (Subtract a multiple of one row from another.) Tj ' +
      '0 -30 Td (Elementary row operations preserve the solution set.) Tj ET'
    : 'BT /F1 20 Tf 50 720 Td (Synthetic Linear Algebra Lecture) Tj ' +
      '0 -40 Td /F1 14 Tf (Matrices and multiplication) Tj ' +
      '0 -30 Td /F1 14 Tf (For compatible matrices, AB combines rows of A with columns of B.) Tj ' +
      '0 -30 Td (Order matters: AB need not equal BA.) Tj ' +
      `0 -30 Td (${sessionInverseStatement}) Tj ` +
      '0 -45 Td (Recall: explain the order using a two-by-two example.) Tj ET';
  // Real PDF drawing work leaves a cancellable render in flight during the document-switch scenario.
  const stream = text + (busy ? '\nq .9 .9 .9 rg 40 200 530 200 re f Q'.repeat(50_000) : '');
  const objects = [
    '<< /Type /Catalog /Pages 2 0 R >>',
    '<< /Type /Pages /Kids [3 0 R] /Count 1 >>',
    '<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] ' +
      '/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>',
    '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>',
    `<< /Length ${stream.length} >>\nstream\n${stream}\nendstream`,
  ];
  let pdf = '%PDF-1.4\n';
  const offsets = [0];
  objects.forEach((object, index) => {
    offsets.push(pdf.length);
    pdf += `${index + 1} 0 obj\n${object}\nendobj\n`;
  });
  const xref = pdf.length;
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  pdf += offsets
    .slice(1)
    .map((offset) => `${String(offset).padStart(10, '0')} 00000 n \n`)
    .join('');
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
  return new TextEncoder().encode(pdf);
}

const pageInsight: PagesPayload = {
  pages: [
    {
      page_number: 1,
      status: 'ready',
      insight: {
        summary: 'Synthetic lecture: matrix multiplication composes transformations; the order changes the result.',
        key_points: ['Match the inner dimensions before multiplying.', 'AB and BA are generally different.'],
        formulas: ['(AB)ij = sum over k of Aik Bkj'],
        assessment_clues: ['Use a concrete two-by-two example to explain non-commutativity.'],
      },
    },
  ],
};

type SessionState =
  | 'ready'
  | 'loading'
  | 'reader-loading'
  | 'reader-recovery'
  | 'document-switch'
  | 'empty-document'
  | 'context-error';

function sessionContext(state: SessionState): SessionContext {
  return {
    course: { name: 'Linear Algebra', short_name: 'MATH101' },
    action: studyAction,
    unit_key: 'unit-101-matrices',
    plan_revision: studyAction.plan_revision,
    annotations_document_id: studyDocumentId,
    annotations: [],
    documents:
      state === 'empty-document'
        ? []
        : [
            {
              document_id: studyDocumentId,
              display_name: 'Matrices — synthetic lecture',
              file_name: 'Matrices.pdf',
              page_count: 1,
              renderable: true,
              document_kind: 'pdf',
              content_url: `${sessionContentPath}?course_id=101`,
              download_url: `${sessionContentPath}?course_id=101`,
            },
            ...(state === 'document-switch'
              ? [
                  {
                    document_id: alternateDocumentId,
                    display_name: alternateDocumentName,
                    file_name: 'Row-reduction.pdf',
                    page_count: 1,
                    renderable: true,
                    document_kind: 'pdf',
                    content_url: `${alternateContentPath}?course_id=101`,
                    download_url: `${alternateContentPath}?course_id=101`,
                  },
                ]
              : []),
          ],
  };
}

function annotationHandlers(initial: Annotation[] = []) {
  let annotations = [...initial];
  return [
    http.get('/api/study/annotations', () => HttpResponse.json({ annotations })),
    http.post('/api/study/annotations', async ({ request }) => {
      const body = annotationCreateSchema.parse(await request.json());
      const annotation: Annotation = {
        id: `annotation-101-${annotations.length + 1}`,
        kind: body.kind,
        status: 'active',
        page_number: body.page_number,
        body: body.body,
        quote: body.quote,
        rects: body.rects,
        char_start: body.char_start,
        char_end: body.char_end,
        prefix: body.prefix,
        suffix: body.suffix,
        color: body.color,
      };
      annotations = [...annotations, annotation];
      return HttpResponse.json({ annotation });
    }),
    http.patch('/api/study/annotations/:id', async ({ request, params }) => {
      const patch = annotationUpdateSchema.parse(await request.json());
      const current = annotations.find((item) => String(item.id) === params.id);
      if (!current) return HttpResponse.json({ detail: 'Synthetic annotation not found.' }, { status: 404 });
      const annotation: Annotation = { ...current, ...patch };
      annotations = annotations.map((item) => (item.id === current.id ? annotation : item));
      return HttpResponse.json({ annotation });
    }),
    http.delete('/api/study/annotations/:id', ({ params }) => {
      annotations = annotations.filter((item) => String(item.id) !== params.id);
      return new HttpResponse(null, { status: 204 });
    }),
  ];
}

function contentResponse(request: Request, alternate = false, busy = false) {
  const bytes = syntheticPdf(alternate, busy);
  const range = /^bytes=(\d+)-(\d*)$/.exec(request.headers.get('range') || '');
  const headers = {
    'Content-Type': 'application/pdf',
    'Accept-Ranges': 'bytes',
    'Content-Disposition': 'inline; filename="Matrices-synthetic.pdf"',
  };
  if (!range) return new HttpResponse(bytes, { headers: { ...headers, 'Content-Length': String(bytes.length) } });
  const first = Number(range[1]);
  const last = Math.min(range[2] ? Number(range[2]) : bytes.length - 1, bytes.length - 1);
  const slice = bytes.slice(first, last + 1);
  return new HttpResponse(slice, {
    status: 206,
    headers: {
      ...headers,
      'Content-Range': `bytes ${first}-${last}/${bytes.length}`,
      'Content-Length': String(slice.length),
    },
  });
}

function practiceHandlers() {
  let practice: PracticeView = {
    enabled: true,
    totals: { due: 1, learned: 0, questions: 1 },
    units: [
      {
        title: 'Matrices and linear transformations',
        questions: [
          {
            question_id: 'question-101-matrix-order',
            unit_key: 'unit-101-matrices',
            queue_rank: 1,
            prompt: 'Why can AB differ from BA?',
            expected_answer: 'Matrix multiplication composes transformations in a specific order.',
            answer_points: ['Changing the order can change the transformation.'],
            state: 'due',
            attempts: 0,
            difficulty: 'introductory',
            response_mode: 'free_text',
          },
        ],
      },
    ],
  };
  return [
    http.get('/api/study/practice', () => HttpResponse.json(practice)),
    http.post('/api/study/practice/attempt', () => {
      practice = { enabled: true, units: [], totals: { due: 0, learned: 1, questions: 1 } };
      return HttpResponse.json({ practice });
    }),
  ];
}

export function sessionFixture(state: SessionState = 'ready'): ReviewFixture & { release: () => void } {
  let release = () => {};
  return {
    id: `synthetic-session-${state}-v1`,
    release: () => release(),
    handlers: () => {
      const pending = new Promise<void>((resolve) => {
        release = resolve;
      });
      const session: SessionInfo = { id: 'session-101-synthetic-resumed', active_seconds: 180 };
      let contentReads = 0;
      return [
        ...studyFixture().handlers(),
        ...annotationHandlers(),
        ...practiceHandlers(),
        http.get('/api/study/session/context', async () => {
          if (state === 'loading') await pending;
          if (state === 'context-error') {
            return HttpResponse.json({ detail: 'Synthetic document source is unavailable.' }, { status: 503 });
          }
          return HttpResponse.json(sessionContext(state));
        }),
        http.get(sessionContentPath, async ({ request }) => {
          if (state === 'reader-loading') await pending;
          if (state === 'reader-recovery' && contentReads++ === 0) {
            return HttpResponse.json({ detail: 'Synthetic document storage is unavailable.' }, { status: 503 });
          }
          return contentResponse(request, false, state === 'document-switch');
        }),
        http.get(alternateContentPath, ({ request }) => contentResponse(request, true)),
        http.get(`/api/study/document/${studyDocumentId}/pages`, () => HttpResponse.json(pageInsight)),
        http.get(`/api/study/document/${alternateDocumentId}/pages`, () => HttpResponse.json({ pages: [] })),
        http.post('/api/study/session/start', () => HttpResponse.json({ session })),
        http.post('/api/study/session/heartbeat', () => HttpResponse.json({ session })),
        http.post('/api/study/session/finish', () => HttpResponse.json(session)),
      ];
    },
  };
}
