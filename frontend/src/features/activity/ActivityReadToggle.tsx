import { storageKey } from '@/lib/browserStorage';
import * as stylex from '@stylexjs/stylex';
import { useCallback, useEffect, useState, type MouseEvent } from 'react';
import { Icon } from '@/components/Icon';
import { colors } from '@/styles/tokens.stylex';
import { readActivityIds } from './activityFeed';

const styles = stylex.create({
  activityMarkRead: {
    background: 'none',
    padding: 0,
    borderStyle: 'none',
    alignItems: 'center',
    color: {
      default: colors.textSecondary,
      ':hover': colors.textPrimary,
    },
    cursor: 'pointer',
    display: 'inline-flex',
    justifyContent: 'center',
  },
  activityMarkReadActive: {
    color: colors.success,
  },
});

function toggleStoredReadId(groupId: string | number): boolean {
  const current = readActivityIds();
  const nextRead = !current.has(groupId);
  if (nextRead) current.add(groupId);
  else current.delete(groupId);
  try {
    localStorage.setItem(storageKey('treeeclass:activity-read'), JSON.stringify([...current]));
  } catch {
    // Quota or private mode fallback
  }
  return nextRead;
}

export function ActivityReadToggle({ groupId, title }: { groupId: string | number; title: string }) {
  const [isRead, setIsRead] = useState(false);

  useEffect(() => {
    setIsRead(readActivityIds().has(groupId));
  }, [groupId]);

  const handleToggle = useCallback(
    (event: MouseEvent<HTMLButtonElement>) => {
      const nextRead = toggleStoredReadId(groupId);
      setIsRead(nextRead);
      const article = event.currentTarget.closest('article');
      if (article) {
        article.dataset.read = nextRead ? 'true' : 'false';
      }
    },
    [groupId],
  );

  return (
    <button
      type="button"
      {...stylex.props(styles.activityMarkRead, isRead && styles.activityMarkReadActive)}
      aria-pressed={isRead}
      title={isRead ? `Mark ${title} unread` : `Mark ${title} read`}
      aria-label={isRead ? `Mark ${title} unread` : `Mark ${title} read`}
      onClick={handleToggle}
    >
      <Icon name={isRead ? 'mail-open' : 'check2'} aria-hidden="true" />
    </button>
  );
}
