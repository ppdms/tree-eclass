import { useLoaderData, useParams } from 'react-router';
import ChangeDetailPage from '@/features/activity/ChangeDetailPage';
import type { changeLoader } from '@/app/data';

export default function ChangeRoute() {
  const { courseId, changeNo } = useParams();
  const data = useLoaderData<typeof changeLoader>();
  return (
    <ChangeDetailPage key={courseId + '/' + changeNo} courseId={courseId!} changeNo={changeNo!} initialData={data} />
  );
}
