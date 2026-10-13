import type { ReactNode } from 'react';
import { isRouteErrorResponse, Outlet, useLocation, useNavigation, useRouteError } from 'react-router';
import { ApiError } from '@/lib/api';
import { Text, View } from '../ui/primitives';
import { Button, Notice } from '../ui/controls';
import { colors } from '../ui/palette';
import { useViewport } from './viewport';
import { useRuntimeChanged } from './runtimeState';
import { reloadPage } from './browser';
import { AppLink } from './links';
import { Navigation } from './navigation';

function RuntimeNotice() {
  const changed = useRuntimeChanged();
  if (!changed) return null;
  return (
    <Notice id="runtime-changed" tone="warning">
      <Text style={{ fontSize: 18, fontWeight: 650 }}>This page belongs to an earlier session</Text>
      <Text>
        Tree-eClass has restarted or changed modes. Your last request made no changes. Copy any unsaved text before
        reloading.
      </Text>
      <Button id="runtime-reload" onClick={reloadPage}>
        Reload page
      </Button>
    </Notice>
  );
}

export function AppShell({ children, loaded = true }: { children?: ReactNode; loaded?: boolean }) {
  const location = useLocation();
  const navigation = useNavigation();
  const viewport = useViewport();
  const session = location.pathname.startsWith('/study/session');
  return (
    <View style={{ width: '100%', minHeight: viewport.height, color: colors.text }}>
      <Navigation />
      <View
        id="app-root"
        pending={!loaded}
        focusable={false}
        style={{ width: '100%', minHeight: viewport.height, paddingTop: session ? 0 : 84 }}
      >
        <RuntimeNotice />
        {navigation.state !== 'idle' && <Text style={{ padding: 12, color: colors.muted }}>Opening the page…</Text>}
        {children ?? <Outlet />}
      </View>
    </View>
  );
}

export function RouteError() {
  const error = useRouteError();
  if ((error instanceof ApiError || isRouteErrorResponse(error)) && error.status === 404) return <MissingPage />;
  return (
    <View ready id="route-error" style={{ padding: 24, gap: 16, color: colors.text }}>
      <Text style={{ fontSize: 28, fontWeight: 650 }}>This page could not load</Text>
      <Text>tree-eClass could not load this page. Your saved data has not changed.</Text>
      <Text style={{ color: colors.muted }}>Try again, or continue from Activity or Study.</Text>
      <View style={{ flexDirection: 'row', flexWrap: 'wrap', gap: 12 }}>
        <Button id="route-retry" onClick={reloadPage}>
          Try again
        </Button>
        <AppLink href="/" style={{ padding: 12, color: colors.link }}>
          Activity
        </AppLink>
        <AppLink href="/study" style={{ padding: 12, color: colors.link }}>
          Open Study
        </AppLink>
      </View>
    </View>
  );
}

export function MissingPage() {
  return (
    <View ready id="route-not-found" style={{ padding: 24, gap: 16, color: colors.text }}>
      <Text style={{ fontSize: 28, fontWeight: 650 }}>Page not found</Text>
      <Text>This page is unavailable. Continue from Activity or your study plan.</Text>
      <View style={{ flexDirection: 'row', flexWrap: 'wrap', gap: 12 }}>
        <AppLink href="/" style={{ padding: 12, color: colors.link }}>
          Return to Activity
        </AppLink>
        <AppLink href="/study" style={{ padding: 12, color: colors.link }}>
          Open Study
        </AppLink>
      </View>
    </View>
  );
}
