export const formatMinutes = (seconds: number | null | undefined): string => {
  const total = Math.round((seconds || 0) / 60);
  if (total < 60) return `${total} min`;
  return `${Math.floor(total / 60)} h ${String(total % 60).padStart(2, '0')}`;
};
