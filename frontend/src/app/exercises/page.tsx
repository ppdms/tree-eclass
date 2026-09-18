import { useLoaderData } from 'react-router';
import Page from '@/features/exercises/ExercisesPage';
import type { loadExercises } from '@/app/data';

export default function Route() {
  const data = useLoaderData<typeof loadExercises>();
  return <Page initialData={data} />;
}
