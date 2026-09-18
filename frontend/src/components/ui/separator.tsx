import * as React from 'react';
import { Divider as AstryxDivider } from '@astryxdesign/core/Divider';
import type { StyleXStyles } from '@stylexjs/stylex';

type SeparatorProps = Omit<React.HTMLAttributes<HTMLDivElement>, 'className' | 'style'> & {
  decorative?: boolean;
  orientation?: 'horizontal' | 'vertical';
  style?: StyleXStyles;
};

const Separator = React.forwardRef<HTMLDivElement, SeparatorProps>(
  ({ style, orientation = 'horizontal', decorative: _decorative = true, ...props }, ref) => (
    <AstryxDivider ref={ref} orientation={orientation} xstyle={style} {...props} />
  ),
);
Separator.displayName = 'Separator';

export { Separator };
export type { SeparatorProps };
