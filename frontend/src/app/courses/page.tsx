import { useLoaderData } from 'react-router';
import Page from '@/features/courses/CoursesPage';
import type { loadCourses } from '@/app/data';

export default function Route() {
  const data = useLoaderData<typeof loadCourses>();
  return <Page initialData={data} />;
}
