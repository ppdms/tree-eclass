import * as React from 'react';
import { Center as AstryxCenter } from '@astryxdesign/core/Center';
import { Card as AstryxCard } from '@astryxdesign/core/Card';
import { HStack } from '@astryxdesign/core/HStack';
import { VStack } from '@astryxdesign/core/VStack';
import type { StyleXStyles } from '@stylexjs/stylex';

type Space = 'none' | 'xs' | 'sm' | 'md' | 'lg' | 'xl';
type LayoutProps = Omit<React.HTMLAttributes<HTMLElement>, 'className' | 'style'> & {
  as?: React.ElementType;
  style?: StyleXStyles;
};
type SpacedProps = LayoutProps & { gap?: Space };

const gaps = {
  none: 0,
  xs: 1,
  sm: 2,
  md: 3,
  lg: 4,
  xl: 6,
} satisfies Record<Space, 0 | 1 | 2 | 3 | 4 | 6>;

export function Stack({ gap = 'md', style, ...props }: SpacedProps) {
  return <VStack gap={gaps[gap]} xstyle={style} {...props} />;
}

export function Inline({ gap = 'sm', style, ...props }: SpacedProps) {
  return <HStack gap={gaps[gap]} wrap="wrap" vAlign="center" xstyle={style} {...props} />;
}

export function Center({ style, children, ...props }: LayoutProps & { children?: React.ReactNode }) {
  return (
    <AstryxCenter xstyle={style} {...props}>
      {children}
    </AstryxCenter>
  );
}

export function Surface({ style, ...props }: LayoutProps) {
  return <AstryxCard xstyle={style} {...props} />;
}

export type { LayoutProps, Space, SpacedProps };
