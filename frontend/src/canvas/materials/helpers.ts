import type { ExternalMaterial, ExternalMaterialType } from '@/lib/types';
import { formatFileSize, formatTimestamp } from '@/features/courses/course-files/helpers';

export const MATERIAL_TYPES: ExternalMaterialType[] = [
  'past_paper',
  'student_notes',
  'study_guide',
  'textbook',
  'exercise_solution',
  'lecture_material',
  'other',
];

export const TYPE_LABELS = {
  past_paper: 'Past papers',
  student_notes: 'Student notes',
  study_guide: 'Student-made study guides',
  textbook: 'Textbooks',
  exercise_solution: 'Exercises & solutions',
  lecture_material: 'Lecture & tutorial material',
  other: 'Other material',
} satisfies Record<ExternalMaterialType, string>;

export const TYPE_OPTIONS = MATERIAL_TYPES.map((value) => ({ value, label: TYPE_LABELS[value] }));

export function materialTypeValue(value: string): ExternalMaterialType {
  return MATERIAL_TYPES.find((type) => type === value) || 'other';
}

export function materialsPath(courseId: string | number): string {
  return `/api/v1/courses/${encodeURIComponent(String(courseId))}/materials`;
}

export function materialId(material: ExternalMaterial): string {
  return `material:${encodeURIComponent(String(material.course_id))}:${encodeURIComponent(material.document_id)}`;
}

export function statusLabel(status: string): string {
  if (status === 'ready') return 'Ready';
  if (status === 'pending' || status === 'running') return 'Indexing';
  if (status === 'unsupported') return 'Unsupported';
  return 'Needs attention';
}

export function classificationLabel(source: ExternalMaterial['classification_source']): string {
  if (source === 'ai') return 'AI classified';
  if (source === 'manual') return 'Manually classified';
  if (source === 'pending') return 'AI classification pending';
  return 'Folder fallback';
}

export function materialDetail(material: ExternalMaterial): string {
  return (
    [
      material.document_kind,
      material.page_count ? `${material.page_count} pages` : '',
      formatFileSize(material.source_size_bytes),
      material.source_label || '',
      classificationLabel(material.classification_source),
      material.source_modified_at ? formatTimestamp(material.source_modified_at) : '',
    ]
      .filter(Boolean)
      .join(' · ') || statusLabel(material.status)
  );
}

export function materialMatches(material: ExternalMaterial, query: string): boolean {
  return (
    !query ||
    [
      material.display_name,
      material.source_path,
      TYPE_LABELS[material.material_type],
      material.source_label || '',
    ].some((value) => value.toLowerCase().includes(query))
  );
}

export interface MaterialGroup {
  type: ExternalMaterialType;
  materials: ExternalMaterial[];
}

export function groupMaterials(materials: ExternalMaterial[], search: string): MaterialGroup[] {
  const query = search.trim().toLowerCase();
  const groups = {
    past_paper: new Array<ExternalMaterial>(),
    student_notes: new Array<ExternalMaterial>(),
    study_guide: new Array<ExternalMaterial>(),
    textbook: new Array<ExternalMaterial>(),
    exercise_solution: new Array<ExternalMaterial>(),
    lecture_material: new Array<ExternalMaterial>(),
    other: new Array<ExternalMaterial>(),
  } satisfies Record<ExternalMaterialType, ExternalMaterial[]>;
  for (const material of materials) {
    if (materialMatches(material, query)) groups[material.material_type].push(material);
  }
  return MATERIAL_TYPES.flatMap((type) => {
    const rows = groups[type];
    return rows.length ? [{ type, materials: rows }] : [];
  });
}

export function replaceMaterial(materials: ExternalMaterial[], material: ExternalMaterial): ExternalMaterial[] {
  return [material, ...materials.filter((row) => row.document_id !== material.document_id)];
}
