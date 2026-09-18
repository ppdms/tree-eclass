import { useEffect, useState } from 'react';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Button } from '@/components/ui/button';

export function RuntimeNotice() {
  const [changed, setChanged] = useState(false);
  useEffect(() => {
    const notify = () => setChanged(true);
    window.addEventListener('tree-runtime-changed', notify);
    return () => window.removeEventListener('tree-runtime-changed', notify);
  }, []);
  if (!changed) return null;
  return (
    <Alert role="alert">
      <AlertTitle>This page belongs to an earlier session</AlertTitle>
      <AlertDescription>
        Tree-eClass has restarted or changed modes. Your last request made no changes. Copy any unsaved text before
        reloading.
      </AlertDescription>
      <Button type="button" onClick={() => window.location.reload()}>
        Reload page
      </Button>
    </Alert>
  );
}
