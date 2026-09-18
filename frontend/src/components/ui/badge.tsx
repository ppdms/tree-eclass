import * as React from 'react';
import { Badge as AstryxBadge } from '@astryxdesign/core/Badge';
import type { StyleXStyles } from '@stylexjs/stylex';

type BadgeVariant = 'default' | 'secondary' | 'destructive' | 'outline';
type BadgeProps = Omit<React.HTMLAttributes<HTMLSpanElement>, 'className' | 'style'> & {
  style?: StyleXStyles;
  variant?: BadgeVariant;
};

const variants = {
  default: 'info',
  secondary: 'neutral',
  destructive: 'error',
  outline: 'neutral',
} as const;

function Badge({ children, style, variant = 'default', ...props }: BadgeProps) {
  return <AstryxBadge label={children} variant={variants[variant]} xstyle={style} {...props} />;
}

export { Badge };
export type { BadgeProps, BadgeVariant };
