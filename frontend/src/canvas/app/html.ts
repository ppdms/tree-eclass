import type { RichBlock } from '../ui/richTypes';
import { emit } from './protocol';

let nextId = 0;
type Completion = (blocks: RichBlock[], error?: string) => void;
const pending = new Map<number, Completion>();

export function requestHTML(value: string, complete: Completion): () => void {
  const id = ++nextId;
  pending.set(id, complete);
  emit({ type: 'html', id, value });
  return () => {
    pending.delete(id);
  };
}

export function completeHTML(id: number, blocks: RichBlock[], error?: string): void {
  const request = pending.get(id);
  if (!request) return;
  pending.delete(id);
  request(blocks, error);
}
