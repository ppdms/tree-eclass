import type { CourseDetailPayload, NavigationCourses } from '@/lib/types';

const progress = new Set<(overview: CourseDetailPayload) => void>();
const catalog = new Set<() => void>();
const order = new Set<(courses: NavigationCourses['courses']) => void>();

export function publishCourseOrder(courses: NavigationCourses['courses']): void {
  for (const listener of order) listener(courses);
}

export function subscribeCourseOrder(listener: (courses: NavigationCourses['courses']) => void): () => void {
  order.add(listener);
  return () => order.delete(listener);
}

export function publishCourseProgress(overview: CourseDetailPayload): void {
  for (const listener of progress) listener(overview);
}

export function subscribeCourseProgress(listener: (overview: CourseDetailPayload) => void): () => void {
  progress.add(listener);
  return () => progress.delete(listener);
}

export function publishCatalogChanged(): void {
  for (const listener of catalog) listener();
}

export function subscribeCatalogChanged(listener: () => void): () => void {
  catalog.add(listener);
  return () => catalog.delete(listener);
}
