import { useLoaderData, useParams } from 'react-router';
import CourseDetailPage from '@/features/courses/CourseDetailPage';
import type { courseLoader } from '@/app/data';

export default function CourseRoute() {
  const { courseId } = useParams();
  const data = useLoaderData<typeof courseLoader>();
  return <CourseDetailPage key={courseId} courseId={courseId!} initialData={data} />;
}
