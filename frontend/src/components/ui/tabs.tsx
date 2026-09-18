import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { Tab, TabList } from '@astryxdesign/core/TabList';

type TabsContextValue = {
  value: string;
  onChange: (value: string) => void;
  panelId: (value: string) => string;
};

const TabsContext = React.createContext<TabsContextValue | null>(null);

const styles = stylex.create({
  content: {
    marginBlockStart: 8,
  },
});

function useTabsContext(): TabsContextValue {
  const context = React.useContext(TabsContext);
  if (!context) {
    throw new Error('TabsList and TabsContent must be rendered inside Tabs');
  }
  return context;
}

export interface TabsProps {
  value?: string;
  defaultValue?: string;
  onValueChange?: (value: string) => void;
  children: React.ReactNode;
}

export function Tabs({ value: controlledValue, defaultValue = '', onValueChange, children }: TabsProps) {
  const [uncontrolledValue, setUncontrolledValue] = React.useState(defaultValue);
  const idPrefix = React.useId();
  const value = controlledValue ?? uncontrolledValue;
  const onChange = React.useCallback(
    (nextValue: string) => {
      if (controlledValue === undefined) setUncontrolledValue(nextValue);
      onValueChange?.(nextValue);
    },
    [controlledValue, onValueChange],
  );

  const panelId = React.useCallback((tabValue: string) => `${idPrefix}-panel-${tabValue}`, [idPrefix]);
  return <TabsContext.Provider value={{ value, onChange, panelId }}>{children}</TabsContext.Provider>;
}

export type TabsListProps = Omit<React.ComponentProps<typeof TabList>, 'value' | 'onChange' | 'children'> & {
  children: React.ReactNode;
};

export function TabsList({ children, ...props }: TabsListProps) {
  const { value, onChange } = useTabsContext();
  return (
    <TabList value={value} onChange={onChange} role="tablist" {...props}>
      {children}
    </TabList>
  );
}

function textContent(children: React.ReactNode): string {
  return React.Children.toArray(children)
    .map((child) => {
      if (React.isValidElement<{ children?: React.ReactNode }>(child)) {
        return textContent(child.props.children);
      }
      return String(child);
    })
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export type TabsTriggerProps = Omit<React.ComponentProps<typeof Tab>, 'value' | 'label' | 'children'> & {
  value: string;
  children: React.ReactNode;
};

export function TabsTrigger({ value, children, ...props }: TabsTriggerProps) {
  const { panelId } = useTabsContext();
  const icon = React.Children.toArray(children).find(
    (child) => React.isValidElement<{ children?: React.ReactNode }>(child) && child.props.children == null,
  );
  return <Tab value={value} label={textContent(children)} icon={icon} panelId={panelId(value)} {...props} />;
}

export interface TabsContentProps extends Omit<React.HTMLAttributes<HTMLDivElement>, 'className'> {
  value: string;
  forceMount?: boolean;
  className?: string;
}

export function TabsContent({ value, forceMount = false, className, children, ...props }: TabsContentProps) {
  const { value: activeValue, panelId } = useTabsContext();
  if (!forceMount && activeValue !== value) return null;
  const baseClassName = stylex.props(styles.content).className;
  const mergedClassName = [baseClassName, className].filter(Boolean).join(' ');
  return (
    <div
      {...props}
      className={mergedClassName}
      id={panelId(value)}
      hidden={activeValue !== value}
      role="tabpanel"
      data-tab-value={value}
    >
      {children}
    </div>
  );
}
