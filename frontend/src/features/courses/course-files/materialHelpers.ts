import { Archive, BookOpenCheck, ClipboardCheck, Library, ListChecks, NotebookPen, Presentation } from 'lucide-react';
import type { ExternalMaterial, ExternalMaterialType } from '@/lib/types';

export const TYPE_META = {
  past_paper: { label: 'Past papers', icon: BookOpenCheck },
  student_notes: { label: 'Student notes', icon: NotebookPen },
  study_guide: { label: 'Student-made study guides', icon: ClipboardCheck },
  textbook: { label: 'Textbooks', icon: Library },
  exercise_solution: { label: 'Exercises & solutions', icon: ListChecks },
  lecture_material: { label: 'Lecture & tutorial material', icon: Presentation },
  other: { label: 'Other material', icon: Archive },
} satisfies Record<ExternalMaterialType, { label: string; icon: typeof Archive }>;

export const MATERIAL_TYPES: ExternalMaterialType[] = [
  'past_paper',
  'student_notes',
  'study_guide',
  'textbook',
  'exercise_solution',
  'lecture_material',
  'other',
];

export function materialTypeValue(value: string): ExternalMaterialType {
  return MATERIAL_TYPES.find((materialType) => materialType === value) || 'other';
}

export function materialMatches(material: ExternalMaterial, query: string): boolean {
  if (!query) return true;
  return [
    material.display_name,
    material.source_path,
    TYPE_META[material.material_type].label,
    material.source_label || '',
  ].some((value) => value.toLowerCase().includes(query));
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
