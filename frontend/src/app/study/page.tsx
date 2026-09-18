import { useLoaderData, useLocation } from 'react-router';
import Page from '@/features/study';
import type { studyLoader } from '@/app/data';

export default function Route() {
  const data = useLoaderData<typeof studyLoader>();
  const { search } = useLocation();
  return <Page key={search} initialData={data} />;
}
