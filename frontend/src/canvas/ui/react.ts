import { createContext, type Context, type ReactNode } from 'react';
import Reconciler from 'react-reconciler';
import { ConcurrentRoot, DefaultEventPriority, NoEventPriority } from 'react-reconciler/constants';
import { UINode, UITree, type HostKind } from './tree';
import type { HostProps } from './types';

export type CanvasContainer = Pick<
  UITree,
  'create' | 'raw' | 'root' | 'beginCommit' | 'endCommit' | 'insert' | 'detach'
>;

type Config = Reconciler.HostConfig<
  HostKind,
  HostProps,
  CanvasContainer,
  UINode,
  UINode,
  never,
  never,
  never,
  never,
  UINode,
  boolean,
  never,
  number,
  -1,
  null,
  null,
  null,
  never,
  never,
  never
>;
let priority = NoEventPriority;

// React's public Context type omits the private slots used by the pinned reconciler.
function rendererContext(
  context: Context<null> | Reconciler.ReactContext<null>,
): context is Reconciler.ReactContext<null> {
  return '_currentValue' in context && '_currentValue2' in context && '_threadCount' in context;
}

const transitionContext = createContext(null);
if (!rendererContext(transitionContext)) throw new Error('React context is incompatible with this renderer');

const config: Config = {
  rendererVersion: '0.1.0',
  rendererPackageName: 'tree-canvas-spike',
  extraDevToolsConfig: null,
  supportsMutation: true,
  supportsPersistence: false,
  supportsHydration: false,
  isPrimaryRenderer: true,
  supportsMicrotasks: true,
  createInstance: (type, props, tree) => tree.create(type, props),
  createTextInstance: (text, tree, inText) => {
    if (!inText) throw new Error('Canvas strings must be inside Text');
    return tree.raw(text);
  },
  appendInitialChild: (parent, child) => parent.root.insert(parent, child),
  finalizeInitialChildren: () => false,
  shouldSetTextContent: () => false,
  getRootHostContext: () => false,
  getChildHostContext: (_parent, type) => type === 'text',
  getPublicInstance: (instance) => instance,
  prepareForCommit: (tree) => {
    tree.beginCommit();
    return null;
  },
  resetAfterCommit: (tree) => tree.endCommit(),
  scheduleTimeout: (fn, delay) => self.setTimeout(fn, delay),
  cancelTimeout: (id) => clearTimeout(id),
  noTimeout: -1,
  scheduleMicrotask: (fn) => queueMicrotask(fn),
  appendChild: (parent, child) => parent.root.insert(parent, child),
  appendChildToContainer: (tree, child) => tree.insert(tree.root, child),
  insertBefore: (parent, child, before) => parent.root.insert(parent, child, before),
  insertInContainerBefore: (tree, child, before) => tree.insert(tree.root, child, before),
  removeChild: (parent, child) => parent.root.detach(parent, child),
  removeChildFromContainer: (tree, child) => tree.detach(tree.root, child),
  commitUpdate: (node, _type, _old, props) => node.update(props),
  commitTextUpdate: (node, _old, text) => {
    node.value = text;
    node.root.dirty = true;
  },
  resetTextContent: (node) => {
    while (node.children.length) node.root.detach(node, node.children[0]!);
  },
  clearContainer: (tree) => {
    while (tree.root.children.length) tree.detach(tree.root, tree.root.children[0]!);
  },
  hideInstance: (node) => {
    node.hidden = true;
    node.root.dirty = true;
  },
  unhideInstance: (node) => {
    node.hidden = false;
    node.root.dirty = true;
  },
  hideTextInstance: (node) => {
    node.hidden = true;
    node.root.dirty = true;
  },
  unhideTextInstance: (node) => {
    node.hidden = false;
    node.root.dirty = true;
  },
  setCurrentUpdatePriority: (next) => {
    priority = next;
  },
  getCurrentUpdatePriority: () => priority,
  resolveUpdatePriority: () => priority || DefaultEventPriority,
  NotPendingTransition: null,
  HostTransitionContext: transitionContext,
  requestPostPaintCallback: (callback) => requestAnimationFrame(callback),
  shouldAttemptEagerTransition: () => false,
  resolveEventType: () => null,
  resolveEventTimeStamp: () => performance.now(),
  maySuspendCommit: () => false,
  maySuspendCommitOnUpdate: () => false,
  maySuspendCommitInSyncRender: () => false,
  preloadInstance: () => true,
  startSuspendingCommit: () => null,
  waitForCommitToBeReady: () => null,
  getSuspendedCommitReason: () => null,
  // DOM scope/form/blur and native-resource suspension aren't supported by this renderer.
  preparePortalMount: () => {},
  getInstanceFromNode: () => null,
  beforeActiveInstanceBlur: () => {},
  afterActiveInstanceBlur: () => {},
  prepareScopeUpdate: () => {},
  getInstanceFromScope: () => null,
  detachDeletedInstance: () => {},
  resetFormInstance: () => {},
  trackSchedulerEvent: () => {},
  suspendInstance: () => {},
  suspendOnActiveViewTransition: () => {},
  bindToConsole: (_method, args) => () => console.log(...args),
};

const reconciler = Reconciler(config);

export function mount(tree: CanvasContainer, element: ReactNode, onError: (error: Error) => void): void {
  const root = reconciler.createContainer(
    tree,
    ConcurrentRoot,
    null,
    false,
    null,
    'canvas-',
    onError,
    onError,
    onError,
    () => {},
    null,
  );
  reconciler.updateContainer(element, root, null);
}

export function activate(callback: () => void): void {
  reconciler.discreteUpdates(callback, undefined, undefined, undefined, undefined);
}

/** Native controlled editors acknowledge only the value after this synchronous React commit. */
export function commitInput(callback: () => void): void {
  reconciler.flushSyncFromReconciler(callback);
}
