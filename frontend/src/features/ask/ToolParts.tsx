import * as stylex from '@stylexjs/stylex';
import * as React from 'react';
import { ArrowUpRight, BookOpen, Check, TriangleAlert } from 'lucide-react';
import { getToolOrDynamicToolName, type ToolUIPart } from 'ai';
import { z } from 'zod/v4';
import { lookup, type JsonValue } from '@/lib/display';
import { commonStyles } from '@/styles/common';
import { colors, layout, spacing, typography } from '@/styles/tokens.stylex';

const styles = stylex.create({
  toolCall: {
    borderRadius: layout.radiusSmall,
    gap: spacing.xs,
    paddingBlock: '0.2rem',
    paddingInline: spacing.xs,
    alignItems: 'center',
    display: 'inline-flex',
    fontSize: typography.sizeSm,
  },
  toolCallRunning: {
    color: colors.info,
  },
  toolCallFinished: {
    color: colors.textSecondary,
  },
  askSources: {
    padding: spacing.md,
    borderColor: colors.border,
    borderRadius: layout.radius,
    borderStyle: 'solid',
    borderWidth: 1,
    gap: spacing.xs,
    backgroundColor: colors.surfaceRaised,
    display: 'flex',
    flexDirection: 'column',
    width: '100%',
  },
  askSource: {
    gap: spacing.sm,
    textDecoration: 'none',
    alignItems: 'center',
    color: { default: colors.textPrimary, ':hover': colors.info },
    display: 'flex',
    fontSize: typography.sizeSm,
  },
  askSourceKind: {
    borderRadius: layout.radiusSmall,
    paddingBlock: '0.1rem',
    paddingInline: '0.35rem',
    fontSize: typography.sizeXs,
    fontWeight: typography.weightSemibold,
    textTransform: 'uppercase',
  },
  askSourceKindOfficial: {
    backgroundColor: 'rgba(37, 99, 235, 0.15)',
    color: colors.info,
  },
  askSourceKindPastExam: {
    backgroundColor: 'rgba(217, 119, 6, 0.15)',
    color: colors.warning,
  },
  askSourceKindCommunity: {
    backgroundColor: 'rgba(126, 34, 206, 0.15)',
    color: colors.statusAccent,
  },
  askSourceKindConsulted: {
    backgroundColor: 'rgba(113, 113, 122, 0.15)',
    color: colors.textSecondary,
  },
  askSourceTitle: {
    overflow: 'hidden',
    flexGrow: 1,
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  askSourcePage: {
    color: colors.textSecondary,
    fontSize: typography.sizeXs,
  },
  askNoSources: {
    gap: spacing.xs,
    alignItems: 'center',
    color: colors.textSecondary,
    display: 'flex',
    fontSize: typography.sizeSm,
  },
  icon: {
    height: '1rem',
    width: '1rem',
  },
  iconSmall: {
    height: '0.875rem',
    width: '0.875rem',
  },
  textDestructive: {
    color: colors.danger,
  },
});

const TOOL_LABELS = {
  list_courses: 'Listing courses',
  search_course_knowledge: 'Searching course materials',
  get_study_priorities: 'Working out the study plan',
  get_course_study_blueprint: 'Reading the course roadmap',
  get_material_insight: 'Inspecting a document',
  get_page_insight: 'Reading a page analysis',
  read_material: 'Reading source material',
  read_course_messages: 'Reading Discord discussion',
  get_recent_changes: 'Checking recent uploads',
  get_index_status: 'Checking index coverage',
} satisfies Record<string, string>;

const SOURCE_KINDS = {
  official: { label: 'official', style: styles.askSourceKindOfficial },
  'past-exam': { label: 'past exam', style: styles.askSourceKindPastExam },
  community: { label: 'community', style: styles.askSourceKindCommunity },
  consulted: { label: 'course material', style: styles.askSourceKindConsulted },
} as const;

type SourceKind = keyof typeof SOURCE_KINDS;

/**
 * The fields the renderer reads off a tool call's input. The AI SDK types the
 * input as `unknown` (it is provider-defined), so it is parsed with a schema
 * at the SDK boundary rather than cast.
 */
const toolInputSchema = z
  .object({
    document_name: z.string().optional(),
    file_name: z.string().optional(),
    title: z.string().optional(),
    query: z.string().optional(),
    course_name: z.string().optional(),
    document_id: z.string().optional(),
    course_id: z.string().optional(),
    page: z.union([z.string(), z.number()]).optional(),
    page_number: z.union([z.string(), z.number()]).optional(),
    evidence_class: z.string().optional(),
  })
  .partial();

type AskToolInput = z.infer<typeof toolInputSchema>;

function toolInput(value: JsonValue): AskToolInput {
  const parsed = toolInputSchema.safeParse(value);
  return parsed.success ? parsed.data : {};
}

/** The SDK's tool input/output are provider-defined JSON; parse at the edge. */
function partInput(part: ToolUIPart): JsonValue {
  // SAFETY: the SDK types tool input as unknown; it is a JSON value by
  // construction (it round-trips through the wire), so the cast only
  // narrows the representation before schema parsing.
  return part.input as JsonValue;
}

function partOutput(part: ToolUIPart): JsonValue {
  // SAFETY: the SDK types tool output as unknown; it is a JSON value by
  // construction (it round-trips through the wire), so the cast only
  // narrows the representation before schema parsing.
  return part.output as JsonValue;
}

/**
 * The AI SDK v5 builds live tool parts as `{type: 'tool-<name>', ...}` and
 * does not store a separate `toolName` field; restored conversations do
 * carry one (added server-side). Deriving from `type` covers both shapes.
 */
function toolName(part: ToolUIPart): string {
  return String(getToolOrDynamicToolName(part));
}

function toolLabel(part: ToolUIPart): string {
  const name = toolName(part).replace(/^tool-/, '');
  return lookup(TOOL_LABELS, name, `Consulting ${name.replace(/_/g, ' ')}`);
}

/**
 * One lookup, shown while it runs rather than after it finishes.
 *
 * The old page held a single static line through every round of the tool
 * loop, so a question that consulted six sources looked identical to one that
 * had hung.
 */
export function ToolCall({ part }: { part: ToolUIPart }) {
  const running = part.state !== 'output-available';
  const failed = isToolOutputFailed(partOutput(part));
  return (
    <div {...stylex.props(styles.toolCall, running ? styles.toolCallRunning : styles.toolCallFinished)}>
      {failed ? (
        <TriangleAlert {...stylex.props(styles.icon, styles.textDestructive)} aria-hidden="true" />
      ) : running ? (
        <BookOpen {...stylex.props(styles.icon)} aria-hidden="true" />
      ) : (
        <Check {...stylex.props(styles.icon)} aria-hidden="true" />
      )}
      <span>{toolLabel(part)}</span>
      {running && <span {...stylex.props(commonStyles.srOnly)}>in progress</span>}
      {failed && <span {...stylex.props(styles.textDestructive)}>— unavailable</span>}
    </div>
  );
}

const failedOutputSchema = z.object({ failed: z.unknown() }).partial();

function isToolOutputFailed(output: JsonValue): boolean {
  const parsed = failedOutputSchema.safeParse(output);
  return parsed.success && Boolean(parsed.data.failed);
}

function sourceTitle(part: ToolUIPart): string {
  const input = toolInput(partInput(part));
  return input.document_name || input.file_name || input.title || input.query || input.course_name || toolLabel(part);
}

function sourceKind(part: ToolUIPart): SourceKind {
  const input = toolInput(partInput(part));
  if (input.evidence_class === 'official_material') return 'official';
  if (input.evidence_class === 'past_exam') return 'past-exam';
  if (input.evidence_class === 'community_file' || toolName(part).includes('messages')) {
    return 'community';
  }
  return 'consulted';
}

function SourceLink({ part, index, content }: { part: ToolUIPart; index: number; content: React.ReactNode }) {
  const input = toolInput(partInput(part));
  const documentId = input.document_id;
  const courseId = input.course_id;
  const page = Number(input.page || input.page_number || 0);
  const href =
    documentId && courseId
      ? `/study/session?course_id=${encodeURIComponent(courseId)}` +
        `&document_id=${encodeURIComponent(documentId)}${page ? `&page=${page}` : ''}`
      : null;
  return href ? (
    <a {...stylex.props(styles.askSource)} href={href} key={`${href}-${index}`}>
      {content}
      <ArrowUpRight {...stylex.props(styles.iconSmall)} aria-hidden="true" />
    </a>
  ) : (
    <span {...stylex.props(styles.askSource)} key={`${sourceTitle(part)}-${index}`}>
      {content}
    </span>
  );
}

export function SourceBlock({ parts }: { parts: ToolUIPart[] }) {
  const successful = parts.filter((part) => !isToolOutputFailed(partOutput(part)));
  return (
    <section {...stylex.props(styles.askSources)} aria-label="Course lookups">
      <strong>Course lookups</strong>
      {successful.length ? (
        successful.map((part, index) => {
          const input = toolInput(partInput(part));
          const page = Number(input.page || input.page_number || 0);
          const kind = sourceKind(part);
          const { style, label } = SOURCE_KINDS[kind];
          const content = (
            <>
              <span {...stylex.props(styles.askSourceKind, style)}>{label}</span>
              <span {...stylex.props(styles.askSourceTitle)}>{sourceTitle(part)}</span>
              {page > 0 && <span {...stylex.props(styles.askSourcePage)}>p. {page}</span>}
            </>
          );
          return <SourceLink part={part} index={index} content={content} />;
        })
      ) : (
        <span {...stylex.props(styles.askNoSources)}>
          <TriangleAlert {...stylex.props(styles.icon)} aria-hidden="true" /> No successful course lookup was recorded.
        </span>
      )}
    </section>
  );
}
