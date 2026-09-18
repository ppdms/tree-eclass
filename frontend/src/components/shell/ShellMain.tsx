import { useLocation } from 'react-router';
import type { ReactNode } from 'react';
import * as stylex from '@stylexjs/stylex';
import { styles as shellStyles } from '@/styles/shell';

export function ShellMain({ children }: { children: ReactNode }) {
  const pathname = useLocation().pathname;
  const isSession = pathname.startsWith('/study/session');
  return (
    <main
      className={stylex.props(shellStyles.main, isSession && shellStyles.sessionMain).className}
      id="app-root"
      tabIndex={-1}
    >
      {children}
    </main>
  );
}
