import type { FromApp, NativeEditInput } from '../app/protocol';
import type { NativeFieldDescriptor, NativeHotspot } from '../app/browserProtocol';
import type { FieldPaint } from './fieldPaint';
import { exposedRects } from './query';
import { activate, commitInput } from './react';
import type { UINode, UITree } from './tree';

export type NativeFields = Extract<FromApp, { type: 'fields' }>;

function descriptor(node: UINode): NativeFieldDescriptor {
  const field = node.props.field!;
  const { onChange: _change, onBlur: _blur, onSubmit: _submit, onMetrics: _metrics, ...options } = field;
  const size = node.style.fontSize ?? 16;
  return {
    ...options,
    ...node.screen,
    id: node.props.id!,
    instance: node.instance,
    disabled: node.disabled,
    editRevision: node.editRevision,
    fontSize: size,
    fontWeight: node.style.fontWeight,
    mono: node.style.mono,
    inset: node.inset.left,
    paddingY: node.inset.top,
    paddingRight: node.inset.right,
    paddingBottom: node.inset.bottom,
    textAlign: node.style.textAlign,
    lineHeight: size * (node.style.lineHeight ?? 1.35),
  };
}

function action(node: UINode): NativeHotspot | null {
  const native = node.props.nativeAction;
  if (!native || !node.props.id) return null;
  const common = { ...node.screen, id: node.props.id, disabled: node.disabled };
  if (native.kind === 'file')
    return { ...common, kind: 'file', accept: native.accept, multiple: native.multiple, resetKey: native.resetKey };
  if (native.kind === 'copy') return { ...common, kind: 'copy', value: native.value };
  return { ...common, kind: 'focus-field', fieldId: native.fieldId };
}

export class NativeInputs {
  constructor(
    readonly tree: UITree,
    readonly paint: FieldPaint,
    readonly invalidate: () => void,
  ) {}

  snapshot(): NativeFields {
    const items: NativeFieldDescriptor[] = [];
    const actions: NativeHotspot[] = [];
    const hotspots: NativeHotspot[] = [];
    for (const node of this.tree.nodes.values()) {
      if (!node.props.id) continue;
      let target = action(node);
      if (node.props.field) {
        items.push(descriptor(node));
        target = { ...node.screen, id: node.props.id, kind: 'text', disabled: node.disabled };
      } else if (target) actions.push(target);
      if (target && !node.disabled) {
        for (const rect of exposedRects(this.tree, node)) hotspots.push({ ...target, ...rect });
      }
    }
    this.paint.retain(this.tree);
    return { type: 'fields', items, actions, hotspots };
  }

  edit(input: NativeEditInput): void {
    const node = this.tree.byId.get(input.field);
    if (!node || input.instance !== node.instance || input.revision < node.editRevision) return;
    if (!input.active && this.paint.state?.field === input.field) {
      this.paint.state = null;
      this.invalidate();
    }
    if (!node?.props.field || node.disabled) return;
    const field = node.props.field;
    if (!field.readOnly && input.value !== field.value) commitInput(() => field.onChange(input.value));
    // Also acknowledge selection/composition/scroll-only edits; otherwise a later controlled clear is blocked.
    node.editRevision = input.revision;
    if (input.active) this.paint.state = input;
    else if (this.paint.state?.field === input.field) this.paint.state = null;
    this.invalidate();
  }

  fieldAction(id: string, instance: number, event: 'focus' | 'blur' | 'submit'): boolean {
    const node = this.tree.byId.get(id);
    if (!node?.props.field || node.disabled || node.instance !== instance) return false;
    if (event === 'blur' && this.paint.state?.field === id) {
      this.paint.state = null;
      this.invalidate();
    }
    if (event === 'blur') {
      const callback = node.props.field.onBlur;
      if (callback) activate(callback);
    }
    if (event === 'submit') {
      const callback = node.props.field.onSubmit;
      if (callback) activate(callback);
    }
    this.invalidate();
    return true;
  }

  metrics(items: { id: string; instance: number; scrollHeight: number; clientHeight: number }[]): void {
    for (const item of items) {
      const node = this.tree.byId.get(item.id);
      const callback = node?.instance === item.instance ? node.props.field?.onMetrics : undefined;
      if (callback) activate(() => callback(item));
    }
  }

  files(id: string, files: File[]): void {
    const node = this.tree.byId.get(id);
    const native = node?.props.nativeAction;
    if (node && !node.disabled && native?.kind === 'file') activate(() => native.onFiles(files));
  }

  result(id: string, error: string | null): void {
    const native = this.tree.byId.get(id)?.props.nativeAction;
    if (native?.kind === 'copy' && native.onResult) activate(() => native.onResult!(error));
  }
}
