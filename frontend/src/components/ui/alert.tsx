import * as React from 'react';
import { Banner } from '@astryxdesign/core/Banner';
import * as stylex from '@stylexjs/stylex';
import type { StyleXStyles } from '@stylexjs/stylex';

type AlertVariant = 'default' | 'destructive';
type AlertProps = Omit<React.HTMLAttributes<HTMLDivElement>, 'className' | 'style' | 'children'> & {
  children?: React.ReactNode;
  style?: StyleXStyles;
  variant?: AlertVariant;
};
type AlertTitleProps = Omit<React.HTMLAttributes<HTMLHeadingElement>, 'className' | 'style'> & {
  style?: StyleXStyles;
};
type AlertDescriptionProps = Omit<React.HTMLAttributes<HTMLDivElement>, 'className' | 'style'> & {
  style?: StyleXStyles;
};

function Alert({ children, style, variant = 'default', ...props }: AlertProps) {
  return (
    <Banner
      status={variant === 'destructive' ? 'error' : 'info'}
      title={null}
      description={children}
      icon={null}
      collapsible={false}
      xstyle={style}
      {...props}
    />
  );
}

function AlertTitle({ style, ...props }: AlertTitleProps) {
  return <strong {...stylex.props(style)} {...props} />;
}

function AlertDescription({ children, style, ...props }: AlertDescriptionProps) {
  return (
    <div {...stylex.props(style)} {...props}>
      {children}
    </div>
  );
}

export { Alert, AlertTitle, AlertDescription };
export type { AlertProps, AlertVariant };
