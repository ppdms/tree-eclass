import { useNavigate } from 'react-router';
import { Text, View } from '../ui/primitives';
import type { HostProps } from '../ui/types';

const routes = new RegExp(
  '^/(?:activity|announcements|timeline|history|courses(?:/[^/]+(?:/changes/[^/]+)?)?' +
    '|exercises|study(?:/session)?|settings|ask)?/?$',
);

export function AppLink({ block = false, ...props }: HostProps & { href: string; block?: boolean }) {
  const navigate = useNavigate();
  const local =
    (props.href.startsWith('#') || routes.test(props.href.split(/[?#]/, 1)[0]!)) && props.download === undefined;
  const Element = block ? View : Text;
  return (
    <Element
      {...props}
      external={!local}
      onClick={
        local && props.target !== '_blank' && !props.disabled
          ? () => {
              props.onClick?.();
              void navigate(props.href);
            }
          : undefined
      }
    />
  );
}
