import { Stack } from '@/components/ui/layout';

export function Loading() {
  return (
    <Stack gap="lg">
      <p role="status" aria-live="polite">
        Opening the page…
      </p>
    </Stack>
  );
}
