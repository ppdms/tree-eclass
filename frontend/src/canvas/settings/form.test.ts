import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { act, createElement, useState, type Dispatch, type SetStateAction } from 'react';
import { mount, type CanvasContainer } from '../ui/react';
import { fetchSettings, useSettingsData, type SettingsLoad, type SettingsResource } from './data';
import { useSettingsForm, type SettingsFormModel } from './form';
import { SettingsContext } from './shared';

class Harness {
  form!: SettingsFormModel;
  resource!: SettingsResource;
  show!: Dispatch<SetStateAction<SettingsLoad | null>>;
}

function hookEnvironment(): () => void {
  const descriptors = ['self', 'IS_REACT_ACT_ENVIRONMENT'].map(
    (key) => [key, Object.getOwnPropertyDescriptor(globalThis, key)] as const,
  );
  Object.assign(globalThis, { self: globalThis, IS_REACT_ACT_ENVIRONMENT: true });
  return () => {
    for (const [key, descriptor] of descriptors) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor);
      else Reflect.deleteProperty(globalThis, key);
    }
  };
}

function settingsContext() {
  return {
    opened: new Set<string>(),
    dirty: new Set<string>(),
    setOpen() {},
    refresh() {},
    observe() {},
    setDirty(id: string, dirty: boolean) {
      if (dirty) this.dirty.add(id);
      else this.dirty.delete(id);
    },
  };
}

function harnessRoot(state: Harness, initial: SettingsLoad) {
  const context = settingsContext();
  function Form({ username }: { username: string }) {
    state.form = useSettingsForm({
      section: 'credentials',
      endpoint: '/api/v1/settings/credentials',
      initial: { username, password: '' },
    });
    return null;
  }
  function Screen({ load }: { load: SettingsLoad }) {
    state.resource = useSettingsData(load);
    return createElement(
      SettingsContext,
      { value: context },
      createElement(Form, { username: state.resource.data.credential_username || '' }),
    );
  }
  function Root() {
    const [load, update] = useState<SettingsLoad | null>(initial);
    state.show = update;
    return load ? createElement(Screen, { key: load.sequence, load }) : null;
  }
  return Root;
}

function hookContainer(): CanvasContainer {
  const children: CanvasContainer['root']['children'] = [];
  const root = new Proxy(
    { children },
    {
      get(target, key) {
        if (key === 'children') return target.children;
        throw new Error(`Hook harness cannot access a host root property: ${String(key)}`);
      },
    },
  );
  const ports = { root, beginCommit() {}, endCommit() {} };
  // SAFETY: both proxies enforce the null-host boundary. Only commit ports and
  // empty root children are available; any graphics/node ownership access throws.
  return new Proxy(ports, {
    get(target, key) {
      if (key === 'root') return target.root;
      if (key === 'beginCommit') return target.beginCommit;
      if (key === 'endCommit') return target.endCommit;
      throw new Error(`Hook harness cannot render a host node: ${String(key)}`);
    },
  }) as CanvasContainer;
}

async function harness(t: TestContext, initial: SettingsLoad): Promise<Harness> {
  const restore = hookEnvironment();
  const state = new Harness();
  t.after(async () => {
    try {
      if (state.show) await act(async () => state.show(null));
    } finally {
      restore();
    }
  });
  const Root = harnessRoot(state, initial);
  await act(async () => {
    mount(hookContainer(), createElement(Root), (error) => {
      throw error;
    });
  });
  assert.ok(state.form && state.resource && state.show, 'The mounted hooks must publish the harness state.');
  return state;
}

function savedSettings(t: TestContext) {
  let username = 'original';
  let resolve!: (response: Response) => void;
  const pending = new Promise<Response>((accept) => {
    resolve = accept;
  });
  t.mock.method(globalThis, 'fetch', async (path: string, options?: RequestInit) => {
    if (options?.method === 'POST') {
      assert.equal(path, '/api/v1/settings/credentials');
      return pending;
    }
    assert.equal(path, '/api/v1/settings');
    return Response.json({ credential_username: username });
  });
  return () => {
    username = 'saved';
    resolve(Response.json({ status: 'ok' }));
  };
}

async function replaceDuringSave(state: Harness): Promise<{ save: Promise<void> }> {
  await act(async () => {
    state.form.change('username', 'saved');
  });
  let save!: Promise<void>;
  await act(async () => {
    save = state.form.submit();
  });
  await act(async () => {
    state.show(null);
  });
  const replacement = await fetchSettings();
  await act(async () => {
    state.show(replacement);
  });
  return { save };
}

test('a save refreshes a pristine replacement without losing catalog defaults', async (t) => {
  const acknowledge = savedSettings(t);
  const state = await harness(t, await fetchSettings());
  const { save } = await replaceDuringSave(state);
  assert.equal(state.form.values.username, 'original');
  await act(async () => {
    state.form.supply({ lazy_model: 'catalog-model' });
  });
  await act(async () => {
    acknowledge();
    await save;
  });
  assert.equal(state.resource.data.credential_username, 'saved');
  assert.deepEqual(state.form.values, { username: 'saved', password: '', lazy_model: 'catalog-model' });
  assert.equal(state.form.state.saved, false, 'the replacement did not submit the old request');
});

test('a late settings save refreshes server state without overwriting newer replacement edits', async (t) => {
  const acknowledge = savedSettings(t);
  const state = await harness(t, await fetchSettings());
  const { save } = await replaceDuringSave(state);
  await act(async () => {
    state.form.change('username', 'newer draft');
  });
  await act(async () => {
    acknowledge();
    await save;
  });
  assert.equal(state.resource.data.credential_username, 'saved');
  assert.equal(state.form.values.username, 'newer draft');
  assert.equal(state.form.state.saved, false);
});
