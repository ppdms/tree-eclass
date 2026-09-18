import { useSearchParams } from 'react-router';
import { SessionIsland } from '@/app/_components/SessionIsland';

export default function SessionRoute() {
  const [params] = useSearchParams();
  const requestedPage = Number(params.get('page'));
  return (
    <SessionIsland
      key={[params.get('course_id'), params.get('action_id'), params.get('document_id')].join(':')}
      courseId={Number(params.get('course_id')) || null}
      actionId={params.get('action_id')}
      documentId={params.get('document_id')}
      initialPageNumber={Number.isInteger(requestedPage) && requestedPage > 0 ? requestedPage : 1}
    />
  );
}
