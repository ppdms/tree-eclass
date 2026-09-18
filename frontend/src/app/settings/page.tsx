import { useLoaderData } from 'react-router';
import Page from '@/features/settings';
import type { loadSettings } from '@/app/data';

export default function Route() {
  const data = useLoaderData<typeof loadSettings>();
  return <Page initialData={data} />;
}
