import { useLoaderData, useLocation } from 'react-router';
import { AskRoute } from '@/features/ask/AskRoute';
import type { askLoader } from '@/app/data';

export default function AskPage() {
  const data = useLoaderData<typeof askLoader>();
  const { search } = useLocation();
  return <AskRoute key={search} query={search.slice(1)} initialData={data} />;
}
