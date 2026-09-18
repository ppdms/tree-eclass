import * as React from 'react';
import * as stylex from '@stylexjs/stylex';
import { Button as AstryxButton } from '@astryxdesign/core/Button';
import type { ButtonProps as AstryxButtonProps } from '@astryxdesign/core/Button';
import type { StyleXStyles } from '@stylexjs/stylex';
import { buttonStyles } from './styles';

type ButtonVariant = 'default' | 'destructive' | 'outline' | 'secondary' | 'ghost' | 'link';
type ButtonSize = 'default' | 'sm' | 'lg' | 'icon';

type ButtonProps = Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, 'className' | 'disabled' | 'style'> & {
  className?: AstryxButtonProps['className'];
  disabled?: boolean;
  endContent?: React.ReactNode;
  href?: string;
  icon?: React.ReactNode;
  isIconOnly?: boolean;
  rel?: string;
  size?: ButtonSize;
  style?: StyleXStyles;
  target?: string;
  type?: 'button' | 'submit' | 'reset';
  variant?: ButtonVariant;
};

const variants = {
  default: 'primary',
  destructive: 'destructive',
  outline: 'secondary',
  secondary: 'secondary',
  ghost: 'ghost',
  link: 'ghost',
} as const;

const sizes = {
  default: 'md',
  sm: 'sm',
  lg: 'lg',
  icon: 'md',
} as const;

const styles = stylex.create({
  smallSize: {
    paddingInline: '0.75rem',
    fontSize: '0.75rem',
    height: '2.25rem',
    minHeight: '2.25rem',
  },
  largeSize: {
    paddingInline: '1.5rem',
    height: '2.75rem',
  },
  iconSize: {
    paddingInline: 0,
    height: '2.75rem',
    minHeight: '2.75rem',
    width: '2.75rem',
  },
});

const sizeStyles = {
  default: null,
  sm: styles.smallSize,
  lg: styles.largeSize,
  icon: styles.iconSize,
} as const;

const variantStyles = {
  default: buttonStyles.primary,
  destructive: null,
  outline: buttonStyles.outline,
  secondary: buttonStyles.secondary,
  ghost: null,
  link: null,
} as const;

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      children,
      style,
      className,
      variant = 'default',
      size = 'default',
      disabled,
      icon,
      endContent,
      isIconOnly: isIconOnlyProp,
      ...props
    },
    ref,
  ) => {
    const isIconOnly = isIconOnlyProp ?? size === 'icon';
    const childLabel = isIconOnly
      ? ''
      : React.Children.toArray(children)
          .filter((child): child is string => typeof child === 'string')
          .join(' ')
          .trim();
    const label = props['aria-label'] || childLabel || 'Action';
    return (
      <AstryxButton
        ref={ref}
        label={label}
        variant={variants[variant]}
        size={sizes[size]}
        isDisabled={disabled}
        isIconOnly={isIconOnly}
        icon={icon}
        endContent={endContent}
        xstyle={[buttonStyles.base, sizeStyles[size], variantStyles[variant], style]}
        className={className}
        {...props}
      >
        {children}
      </AstryxButton>
    );
  },
);
Button.displayName = 'Button';

export { Button };
export type { ButtonProps, ButtonSize, ButtonVariant };
