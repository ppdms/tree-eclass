import { http, HttpResponse } from 'msw';
import { z } from 'zod/v4';
import type { CourseFilesPayload, CoursesCoverage, ExternalMaterial } from '@/lib/types';
import type { ReviewFixture } from '../scenario';
import { activityFixture } from './activity';
import {
  courseCoverage,
  courseFiles,
  courseOverview,
  externalNotes,
  fileMetadata,
  matricesGuide,
  sourceHash,
} from './courses-data';

export function courseShelfFixture(state: 'normal' | 'empty' | 'order-recovery' = 'normal'): ReviewFixture {
  return {
    id: `synthetic-courses-shelf-${state}-v1`,
    handlers: () => {
      let saves = 0;
      return [
        http.get('/api/v1/courses/coverage', () =>
          HttpResponse.json(
            state === 'empty'
              ? ({ courses: [], recent_materials: [], status: 'ready' } satisfies CoursesCoverage)
              : courseCoverage,
          ),
        ),
        http.post('/api/courses/reorder', () => {
          if (state === 'order-recovery' && ++saves === 1) {
            return HttpResponse.json({ detail: 'Synthetic order service unavailable' }, { status: 503 });
          }
          return HttpResponse.json({ status: 'ok' });
        }),
      ];
    },
  };
}

export function unavailableCourseFixture(): ReviewFixture {
  return {
    id: 'synthetic-courses-unavailable-v1',
    handlers: () => [
      http.get('/api/v1/courses/101/overview', () =>
        HttpResponse.json({ detail: 'Course not found' }, { status: 404 }),
      ),
      ...activityFixture().handlers(),
    ],
  };
}

export function courseDetailFixture(state: 'normal' | 'files-recovery' | 'empty-files' = 'normal') {
  let releaseFiles = () => {};
  return {
    id: `synthetic-courses-detail-${state}-v1`,
    releaseFiles: () => releaseFiles(),
    handlers: () => {
      let fileRequests = 0;
      const pending = new Promise<void>((resolve) => {
        releaseFiles = resolve;
      });
      return [
        http.get('/api/v1/courses/101/overview', () => HttpResponse.json(courseOverview)),
        http.get('/api/v1/courses/101/files', async () => {
          if (state === 'files-recovery' && ++fileRequests === 1) {
            await pending;
            return HttpResponse.json({ detail: 'Synthetic files service unavailable' }, { status: 503 });
          }
          return HttpResponse.json(
            state === 'empty-files' ? ({ study_levels: {} } satisfies CourseFilesPayload) : courseFiles,
          );
        }),
        http.get('/api/v1/courses/101/file-metadata', () =>
          HttpResponse.json(state === 'empty-files' ? { file_insights: {} } : fileMetadata),
        ),
        http.get('/api/v1/courses/101/files/document-101-matrices/guide', () =>
          HttpResponse.json({
            source_hash: sourceHash,
            ai: matricesGuide,
          }),
        ),
        http.post('/api/courses/101/files/study-level', () => HttpResponse.json({ status: 'ok' })),
        http.post('/api/courses/101/folders/collapsed', () => HttpResponse.json({ status: 'ok' })),
        ...externalMaterialHandlers(state === 'empty-files'),
      ];
    },
  };
}

function externalMaterialHandlers(empty: boolean) {
  let material: ExternalMaterial = { ...externalNotes };
  return [
    http.get('/api/v1/courses/101/materials', () => HttpResponse.json({ materials: empty ? [] : [material] })),
    http.patch('/api/v1/courses/101/materials/document-101-revision-notes', async ({ request }) => {
      const body = z
        .object({
          material_type: z.enum([
            'past_paper',
            'student_notes',
            'study_guide',
            'textbook',
            'exercise_solution',
            'lecture_material',
            'other',
          ]),
        })
        .parse(await request.json());
      material = { ...material, material_type: body.material_type, classification_source: 'manual' };
      return HttpResponse.json({ material });
    }),
    http.get(
      '/api/study/document/document-101-revision-notes/content',
      () =>
        new HttpResponse(
          'Synthetic matrix revision notes: identify pivots, then state the rank and free variables.\n',
          {
            headers: {
              'Content-Type': 'text/plain; charset=utf-8',
              'Content-Disposition': 'inline; filename="Matrix revision notes.txt"',
            },
          },
        ),
    ),
  ];
}
