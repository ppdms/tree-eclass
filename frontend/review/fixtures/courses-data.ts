import type {
  CourseDetailPayload,
  CourseFilesPayload,
  CoursesCoverage,
  ExternalMaterial,
  FileInsightAi,
} from '@/lib/types';

export const matricesPath = '/Courses/101/eclass/Lectures/Matrices.pdf';
export const sourceHash = 'a'.repeat(64);

export const courseCoverage: CoursesCoverage = {
  status: 'ready',
  generation: 7,
  generated_at: '2026-10-13T09:45:00Z',
  courses: [
    {
      id: 101,
      name: 'Linear Algebra',
      code: 'MATH101',
      short_name: 'MATH101',
      total_files: 8,
      indexed_files: 8,
      completion_ratio: 0.5,
      study_distribution: { 0: 2, 1: 1, 2: 1, 3: 3, 4: 1 },
      webdav_folder: '/Courses/101/eclass',
    },
    {
      id: 202,
      name: 'Algorithms',
      code: 'CS202',
      short_name: 'CS202',
      total_files: 12,
      indexed_files: 10,
      completion_ratio: 0.25,
      study_distribution: { 0: 6, 1: 2, 2: 1, 3: 2, 4: 1 },
    },
    {
      id: 303,
      name: 'Statistics',
      code: 'STAT303',
      short_name: 'STAT303',
      total_files: 6,
      indexed_files: 6,
      completion_ratio: 0.83,
      study_distribution: { 0: 1, 3: 3, 4: 2 },
    },
  ],
  recent_materials: [
    {
      document_id: 'document-101-matrices',
      course_id: 101,
      course_name: 'Linear Algebra',
      source_path: matricesPath,
      display_name: 'Matrices.pdf',
      indexed_at: '2026-10-13T08:30:00Z',
    },
  ],
};

export const courseOverview: CourseDetailPayload = {
  course: { ...courseCoverage.courses[0]!, id: 101 },
  course_blueprint: {
    usable: true,
    total_units: 3,
    readiness: { state: 'ready' },
    progress: {
      percent: 50,
      completed_actions: 6,
      total_actions: 12,
      next_action: {
        action_id: 'synthetic-row-reduction',
        unit_title: 'Matrices and linear systems',
        instruction: 'Practice row reduction and identify pivot columns',
        estimated_minutes: 25,
      },
    },
    blueprint: {
      units: [
        { key: 'vectors', title: 'Vectors and subspaces', completed_actions: 4, total_actions: 4 },
        { key: 'matrices', title: 'Matrices and linear systems', completed_actions: 2, total_actions: 5 },
        { key: 'eigenvalues', title: 'Eigenvalues and diagonalization', completed_actions: 0, total_actions: 3 },
      ],
    },
  },
};

export const courseFiles: CourseFilesPayload = {
  tree: {
    name: 'eClass materials',
    local_path: '/Courses/101/eclass',
    children: [
      {
        name: 'Lectures',
        local_path: '/Courses/101/eclass/Lectures',
        files: [
          { name: 'Matrices.pdf', local_path: matricesPath, file_size: 245760, last_updated: '2026-10-13T08:30:00Z' },
          {
            name: 'Vector-spaces.pdf',
            local_path: '/Courses/101/eclass/Lectures/Vector-spaces.pdf',
            file_size: 153600,
            last_updated: '2026-10-10T12:00:00Z',
          },
        ],
      },
      {
        name: 'Practice',
        local_path: '/Courses/101/eclass/Practice',
        files: [
          { name: 'Row-reduction.pdf', local_path: '/Courses/101/eclass/Practice/Row-reduction.pdf', file_size: 81920 },
          { name: 'Interactive matrix calculator', redirect_url: 'https://example.invalid/algebra/matrix-calculator' },
        ],
      },
    ],
  },
  expanded_folders: ['/Courses/101/eclass/Lectures'],
  study_levels: { [matricesPath]: 2, '/Courses/101/eclass/Lectures/Vector-spaces.pdf': 3 },
  files_with_versions: [],
  folders_with_deleted: [],
  collapsed_folders: [],
};

export const fileMetadata: Pick<CourseFilesPayload, 'file_insights'> = {
  file_insights: {
    [matricesPath]: {
      id: 'document-101-matrices',
      course_id: 101,
      source_hash: sourceHash,
      document_kind: 'pdf',
      source_status: 'ready',
      page_count: 18,
      reading_minutes: 24,
      complexity_label: 'Moderate',
      enrichment_status: 'ready',
      guide_available: true,
      enrichment_model: 'synthetic-review-model',
      enrichment_generated_at: '2026-10-13T09:00:00Z',
      enrichment_analysis_version: '6',
    },
  },
};

export const matricesGuide: FileInsightAi = {
  summary: 'The lecture connects elementary row operations with solving linear systems and finding matrix rank.',
  importance: 'high',
  difficulty: 'moderate',
  assessment_relevance: 'high',
  material_type: 'lecture_material',
  recommended_action: 'Reduce the three example systems and explain which variables are free.',
  learning_objectives: ['Find pivot columns and matrix rank.', 'Distinguish consistent and inconsistent systems.'],
  topics: ['Row reduction', 'Rank', 'Linear systems'],
  prerequisites: ['Matrix multiplication'],
  assessment_reason: 'Midterm questions require a complete row-reduction argument, not only a final answer.',
};

export const externalNotes: ExternalMaterial = {
  id: 'synthetic-external-notes',
  document_id: 'document-101-revision-notes',
  course_id: 101,
  display_name: 'Matrix revision notes.txt',
  source_path: '/Courses/101/external/Matrix revision notes.txt',
  source_origin: 'external',
  material_type: 'student_notes',
  classification_source: 'manual',
  source_label: 'Synthetic learner upload',
  document_kind: 'text',
  status: 'ready',
  renderable: false,
  source_size_bytes: 2048,
  reading_minutes: 8,
  download_url: '/api/study/document/document-101-revision-notes/content?course_id=101',
};
