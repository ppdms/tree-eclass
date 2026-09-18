import { isServer } from '@/lib/display';

// Sanitizes eClass-provided assignment HTML before it is rendered inside the
// exercise detail disclosure. Only a small allowlist of inline elements keeps
// its tag; everything else is unwrapped, event handlers are stripped, and
// anchors keep only safe href schemes.
export function plainExerciseText(value: string): string {
  return String(value || '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function safeExerciseHtml(value: string): string {
  if (!value) return '';
  if (isServer()) return plainExerciseText(value);
  const document = new DOMParser().parseFromString(String(value), 'text/html');
  const allowed = new Set(['A', 'B', 'BR', 'CODE', 'EM', 'I', 'LI', 'OL', 'P', 'PRE', 'STRONG', 'UL']);
  document.body.querySelectorAll('*').forEach((node) => {
    if (!allowed.has(node.tagName)) {
      node.replaceWith(...node.childNodes);
      return;
    }
    [...node.attributes].forEach((attribute) => {
      if (attribute.name.startsWith('on') || !['href', 'title'].includes(attribute.name)) {
        node.removeAttribute(attribute.name);
      }
    });
    if (node.tagName === 'A' && !/^(https?:|mailto:|\/)/i.test(node.getAttribute('href') || '')) {
      node.removeAttribute('href');
    }
  });
  return document.body.innerHTML;
}
