import { useCallback, useEffect, useRef, useState } from 'react';
import { useLocation } from 'react-router';
import { AppLink } from '../app/links';
import { scrollToNode, setUnloadGuard } from '../app/browser';
import { useViewport } from '../app/viewport';
import { ScrollArea } from '../ui/overlays';
import { Panel } from '../ui/controls';
import { Text, View } from '../ui/primitives';
import { colors } from '../ui/palette';
import { sections, styles, useSettingsContext } from './shared';

function useSettingsScrollspy(topBound: number, bottomBound: number) {
  const [active, setActive] = useState('appearance');
  const positions = useRef<Record<string, { y: number; height: number }>>({});
  const observe = useCallback(
    (id: string, rect: { y: number; height: number }) => {
      positions.current[id] = { y: rect.y, height: rect.height };
      const top = topBound + (bottomBound - topBound) * 0.4;
      const bottom = topBound + (bottomBound - topBound) * 0.45;
      const first = sections
        .map(([section]) => ({ id: section, rect: positions.current[section] }))
        .filter((item) => item.rect && item.rect.y < bottom && item.rect.y + item.rect.height > top)
        .sort((a, b) => a.rect!.y - b.rect!.y)[0];
      if (first) setActive((previous) => (previous === first.id ? previous : first.id));
    },
    [topBound, bottomBound],
  );
  return { active, observe };
}
export function useSettingsWorkspace(refresh: () => void) {
  const [opened, setOpened] = useState(new Set<string>());
  const [dirty, setDirtySections] = useState(new Set<string>());
  const viewport = useViewport();
  const location = useLocation();
  const { active, observe } = useSettingsScrollspy(viewport.top, viewport.bottom);
  const setOpen = useCallback((id: string, open: boolean) => {
    setOpened((previous) => {
      if (previous.has(id) === open) return previous;
      const next = new Set(previous);
      if (open) next.add(id);
      else next.delete(id);
      return next;
    });
  }, []);
  const setDirty = useCallback((id: string, isDirty: boolean) => {
    setDirtySections((previous) => {
      if (previous.has(id) === isDirty) return previous;
      const next = new Set(previous);
      if (isDirty) next.add(id);
      else next.delete(id);
      return next;
    });
  }, []);
  useEffect(() => {
    setUnloadGuard('settings-drafts', dirty.size > 0);
    return () => setUnloadGuard('settings-drafts', false);
  }, [dirty.size]);
  useEffect(() => {
    const id = location.hash.slice(1);
    if (sections.some(([section]) => section === id)) {
      setOpen(id, true);
      scrollToNode(id);
    }
  }, [location.hash, setOpen]);
  return { context: { opened, dirty, setOpen, setDirty, refresh, observe }, active, viewport };
}
function NavigationLinks({ active, mobile }: { active: string; mobile: boolean }) {
  const context = useSettingsContext();
  const location = useLocation();
  const links = sections.map(([id, label]) => (
    <AppLink
      key={id}
      id={`settings-nav-${id}`}
      href={`${location.pathname}${location.search}#${id}`}
      block
      title={context.dirty.has(id) ? 'Unsaved changes' : label}
      onClick={() => {
        context.setOpen(id, true);
        scrollToNode(id);
      }}
      style={{
        padding: 10,
        borderRadius: 10,
        minHeight: 38,
        flexShrink: 0,
        maxWidth: mobile ? 260 : '100%',
        backgroundColor: active === id ? colors.raised : colors.surface,
        color: colors.text,
      }}
    >
      <Text>{`${label}${context.dirty.has(id) ? ' ·' : ''}`}</Text>
    </AppLink>
  ));
  return mobile ? <>{links}</> : <View style={styles.stack}>{links}</View>;
}
export function SettingsNavigation({ active }: { active: string }) {
  const viewport = useViewport();
  const mobile = viewport.width <= 900;
  const links = (
    <Panel>
      <Text style={styles.heading}>Settings sections</Text>
      <Text style={styles.muted}>Jump to a workspace area.</Text>
      <NavigationLinks active={active} mobile={mobile} />
    </Panel>
  );
  if (mobile)
    return (
      <Panel>
        <Text style={styles.heading}>Settings sections</Text>
        <Text style={styles.muted}>Jump to a workspace area.</Text>
        <ScrollArea
          id="settings-navigation-rail"
          axis="horizontal"
          height={60}
          style={{ width: '100%', flexDirection: 'row', alignItems: 'start', gap: 12 }}
        >
          <NavigationLinks active={active} mobile />
        </ScrollArea>
        <Text style={styles.muted}>Swipe to see all sections</Text>
      </Panel>
    );
  return (
    <View style={{ width: 232, flexShrink: 0 }}>
      <View fixed style={{ width: 232, left: Math.max(24, (viewport.width - 1180) / 2 + 24), top: 100 }}>
        <ScrollArea id="settings-navigation-scroll" height={Math.max(160, viewport.bottom - 120)}>
          {links}
        </ScrollArea>
      </View>
    </View>
  );
}
