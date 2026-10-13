import {
  Folder as folder,
  FolderOpen as folderOpen,
  File as file,
  FileDiff as fileDiff,
  Megaphone as megaphone,
  Check as check,
  MailOpen as mailOpen,
  Clock as clock,
  ChevronUp as chevronUp,
  ChevronDown as chevronDown,
} from 'lucide';
import type { IconName, Rect } from './types';

const icons = {
  folder,
  'folder-open': folderOpen,
  file,
  'file-diff': fileDiff,
  megaphone,
  check,
  'mail-open': mailOpen,
  clock,
  'chevron-up': chevronUp,
  'chevron-down': chevronDown,
};

export class Icons {
  readonly #paths = new Map<IconName, Path2D>();

  draw(context: OffscreenCanvasRenderingContext2D, name: IconName, rect: Rect, color: string): void {
    let path = this.#paths.get(name);
    if (!path) {
      path = new Path2D();
      for (const [kind, attrs] of icons[name]) {
        let data = attrs.d;
        if (kind === 'circle') {
          const [x, y, r] = [Number(attrs.cx), Number(attrs.cy), Number(attrs.r)];
          data = `M${x - r} ${y}a${r} ${r} 0 1 0 ${2 * r} 0a${r} ${r} 0 1 0 ${-2 * r} 0Z`;
        } else if (kind !== 'path' || !data) throw new Error(`Unsupported primitive in icon ${name}: ${kind}`);
        path.addPath(new Path2D(String(data)));
      }
      this.#paths.set(name, path);
    }
    const size = Math.min(rect.width, rect.height);
    context.save();
    context.translate(rect.x + (rect.width - size) / 2, rect.y + (rect.height - size) / 2);
    context.scale(size / 24, size / 24);
    context.strokeStyle = color;
    context.lineWidth = 1.8;
    context.lineCap = 'round';
    context.lineJoin = 'round';
    context.stroke(path);
    context.restore();
  }
}
