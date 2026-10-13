import { getToolOrDynamicToolName, type DataUIPart, type ToolUIPart, type UIDataTypes } from 'ai';
import { z } from 'zod/v4';
import type { AskBootstrap as SourceBootstrap, Conversation } from '@/features/ask/types';

export type AskBootstrap = Omit<SourceBootstrap, 'conversation_id'> & {
  conversation_id?: string | number | null;
};
export type { Conversation };

const labels = new Map([
  ['list_courses', 'Listing courses'],
  ['search_course_knowledge', 'Searching course materials'],
  ['get_study_priorities', 'Working out the study plan'],
  ['get_course_study_blueprint', 'Reading the course roadmap'],
  ['get_material_insight', 'Inspecting a document'],
  ['get_page_insight', 'Reading a page analysis'],
  ['read_material', 'Reading source material'],
  ['read_course_messages', 'Reading Discord discussion'],
  ['get_recent_changes', 'Checking recent uploads'],
  ['get_index_status', 'Checking index coverage'],
]);

const conversationSchema = z.object({
  id: z.union([z.string(), z.number()]),
  title: z.string().optional().catch(undefined),
  display_title: z.string().optional().catch(undefined),
  updated_at: z.string().optional().catch(undefined),
  last_message_excerpt: z.string().optional().catch(undefined),
});
const errorBodySchema = z.object({ detail: z.unknown().optional() });
const detailItemSchema = z.object({ msg: z.unknown().optional(), message: z.unknown().optional() });

// These are the provider-defined fields the source renderer reads, not a replacement
// payload. Guards below project each field without dropping other tool input keys.
interface AskToolInput {
  document_name?: ToolUIPart['input'];
  file_name?: ToolUIPart['input'];
  title?: ToolUIPart['input'];
  query?: ToolUIPart['input'];
  course_name?: ToolUIPart['input'];
  document_id?: ToolUIPart['input'];
  course_id?: ToolUIPart['input'];
  page?: ToolUIPart['input'];
  page_number?: ToolUIPart['input'];
  evidence_class?: ToolUIPart['input'];
}

function isToolInput(value: ToolUIPart['input']): value is AskToolInput {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isToolText(value: AskToolInput[keyof AskToolInput]): value is string {
  return typeof value === 'string';
}

function isCourseNumber(value: AskToolInput['course_id']): value is number {
  return typeof value === 'number';
}

function hasFailureFlag(value: ToolUIPart['output']): value is { failed: ToolUIPart['output'] } {
  return value !== null && typeof value === 'object' && !Array.isArray(value) && 'failed' in value;
}

export function conversationData(part: DataUIPart<UIDataTypes>): Conversation | null {
  const parsed = conversationSchema.safeParse(part.data);
  return parsed.success && String(parsed.data.id) ? parsed.data : null;
}

export function friendlyError(cause: unknown): string {
  const raw = cause instanceof Error ? cause.message : String(cause || '');
  try {
    const parsed = errorBodySchema.safeParse(JSON.parse(raw));
    if (parsed.success && parsed.data.detail) {
      const detail = parsed.data.detail;
      if (!Array.isArray(detail)) return String(detail);
      return detail
        .map((item) => {
          const parsedItem = detailItemSchema.safeParse(item);
          return String(parsedItem.success ? (parsedItem.data.msg ?? parsedItem.data.message ?? item) : item);
        })
        .join('. ');
    }
  } catch {
    // HTTP errors from the SDK can be plain text; do not hide their message.
  }
  if (/failed to fetch/i.test(raw)) return 'Could not reach the server. Check your connection and try again.';
  return raw || 'The question could not be answered.';
}

export function toolLabel(part: ToolUIPart): string {
  const name = String(getToolOrDynamicToolName(part)).replace(/^tool-/, '');
  return labels.get(name) || `Consulting ${name.replace(/_/g, ' ')}`;
}

export function toolFailed(part: ToolUIPart): boolean {
  return hasFailureFlag(part.output) && Boolean(part.output.failed);
}

function textInput(input: AskToolInput, key: keyof AskToolInput): string {
  const value = input[key];
  return isToolText(value) ? value : '';
}

export function lookupSource(part: ToolUIPart) {
  const input: AskToolInput = isToolInput(part.input) ? part.input : {};
  const title =
    (['document_name', 'file_name', 'title', 'query', 'course_name'] as const)
      .map((key) => textInput(input, key))
      .find(Boolean) || toolLabel(part);
  const pageValue = Number(input.page || input.page_number || 0);
  const page = Number.isFinite(pageValue) && pageValue > 0 ? pageValue : 0;
  const document = textInput(input, 'document_id');
  const course = isCourseNumber(input.course_id) ? String(input.course_id) : textInput(input, 'course_id');
  const kind =
    input.evidence_class === 'official_material'
      ? 'official'
      : input.evidence_class === 'past_exam'
        ? 'past exam'
        : input.evidence_class === 'community_file' || String(getToolOrDynamicToolName(part)).includes('messages')
          ? 'community'
          : 'course material';
  const href =
    document && course
      ? `/study/session?course_id=${encodeURIComponent(course)}&document_id=${encodeURIComponent(document)}` +
        (page ? `&page=${page}` : '')
      : null;
  return { title, page, kind, href };
}

export function displayTitle(item: Conversation): string {
  return item.title || item.display_title || 'Conversation';
}

export function conversationPreview(item: Conversation): string {
  const excerpt = item.last_message_excerpt || '';
  return displayTitle(item).includes(excerpt) ? '' : excerpt;
}

export function relativeTime(value: string | undefined): string {
  const raw = String(value || '').replace(' ', 'T');
  const date = new Date(/(?:Z|[+-]\d\d:\d\d)$/.test(raw) ? raw : `${raw}Z`);
  if (Number.isNaN(date.getTime())) return '';
  const minutes = Math.max(0, Math.round((Date.now() - date.getTime()) / 60000));
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes} min ago`;
  if (minutes < 1440) return `${Math.round(minutes / 60)} hr ago`;
  return `${Math.round(minutes / 1440)} days ago`;
}

export function filterConversations(items: Conversation[], query: string): Conversation[] {
  const normalized = query.trim().toLocaleLowerCase();
  return items.filter((item) =>
    `${item.display_title || item.title || ''} ${item.last_message_excerpt || ''}`
      .toLocaleLowerCase()
      .includes(normalized),
  );
}
