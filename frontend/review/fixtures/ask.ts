import { http, HttpResponse, type HttpHandler } from 'msw';
import { z } from 'zod/v4';
import type { UIMessage, UIMessageChunk } from 'ai';
import type { AskBootstrap, Conversation, ConversationsPayload } from '@/features/ask/types';
import type { ReviewFixture } from '../scenario';

export const matrixQuestion = 'Why does an invertible matrix have a nonzero determinant?';
export const matrixAnswer = 'An invertible matrix has a nonzero determinant because its columns are independent.';
const source = {
  course_id: '101',
  document_id: 'document-101-matrices',
  document_name: 'Matrices.pdf',
  page: 1,
  evidence_class: 'official_material',
};
const conversation: Conversation = {
  id: 701,
  title: 'Matrix inverses',
  display_title: 'Matrix inverses',
  updated_at: '2026-10-13T10:00:00Z',
  last_message_excerpt: matrixAnswer,
};
const restoredMessages: UIMessage[] = [
  { id: 'synthetic-question-701', role: 'user', parts: [{ type: 'text', text: matrixQuestion }] },
  {
    id: 'synthetic-answer-701',
    role: 'assistant',
    parts: [
      {
        type: 'tool-read_material',
        toolCallId: 'lookup-701',
        state: 'output-available',
        input: source,
        output: { failed: false },
      },
      { type: 'text', text: matrixAnswer },
    ],
  },
];
type AskState = 'new' | 'saved' | 'offline' | 'http-error' | 'stream-error' | 'missing' | 'history-error';

function completionGate() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

function startingChunks(turn: number): UIMessageChunk[] {
  return [
    { type: 'start', messageId: `synthetic-answer-${turn}` },
    { type: 'start-step' },
    { type: 'tool-input-available', toolCallId: `lookup-${turn}`, toolName: 'read_material', input: source },
  ];
}

function finishingChunks(turn: number, text: string): UIMessageChunk[] {
  return [
    { type: 'tool-output-available', toolCallId: `lookup-${turn}`, output: { failed: false } },
    { type: 'text-start', id: `text-${turn}` },
    { type: 'text-delta', id: `text-${turn}`, delta: text },
    { type: 'text-end', id: `text-${turn}` },
    { type: 'data-conversation', data: conversation },
    { type: 'finish-step' },
    { type: 'finish' },
  ];
}

function answerStream(turn: number, finish: UIMessageChunk[], gate: Promise<void>) {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    async start(controller) {
      const send = (chunk: UIMessageChunk) => {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`));
      };
      startingChunks(turn).forEach(send);
      await gate;
      finish.forEach(send);
      controller.enqueue(encoder.encode('data: [DONE]\n\n'));
      controller.close();
    },
  });
  return new HttpResponse(body, {
    headers: { 'Content-Type': 'text/event-stream; charset=utf-8', 'X-Vercel-AI-UI-Message-Stream': 'v1' },
  });
}

function bootstrap(state: AskState, saved: boolean): AskBootstrap {
  return {
    conversation_id: saved ? 701 : undefined,
    initial_messages: saved ? restoredMessages : [],
    examples: [matrixQuestion, 'Explain row reduction using the lecture notes.'],
    models: state === 'offline' ? [] : ['synthetic-review-model', 'synthetic-review-small'],
    default_model: 'synthetic-review-model',
    provider_available: state !== 'offline',
  };
}

function askHandlers(state: AskState, ready: Promise<void>): HttpHandler[] {
  let turns = 0;
  let historyRequests = 0;
  let saved = state === 'saved';
  return [
    http.get('/api/ask/bootstrap', ({ request }) => {
      const selected = new URL(request.url).searchParams.has('conversation_id');
      if (state === 'missing' && selected)
        return HttpResponse.json({ detail: 'Conversation not found' }, { status: 404 });
      return HttpResponse.json(bootstrap(state, selected && saved));
    }),
    http.get('/api/ask/conversations', () => {
      if (state === 'history-error' && ++historyRequests === 1)
        return HttpResponse.json({ detail: 'Conversation history unavailable' }, { status: 503 });
      return HttpResponse.json({ conversations: saved ? [conversation] : [] } satisfies ConversationsPayload);
    }),
    http.post('/api/ask/stream', async ({ request }) => {
      const body = z
        .object({
          question: z.string(),
          model: z.string(),
          conversation_id: z.number().nullable().optional(),
        })
        .parse(await request.json());
      turns += 1;
      if (state === 'http-error' && turns === 1) {
        return HttpResponse.json(
          { detail: 'No answer model is currently available. Try again later.' },
          { status: 503 },
        );
      }
      if (state === 'stream-error' && turns === 1) {
        const finish: UIMessageChunk[] = [
          { type: 'error', errorText: 'The answer model could not complete this question. Try again.' },
          { type: 'finish-step' },
          { type: 'finish' },
        ];
        return answerStream(turns, finish, ready);
      }
      saved = true;
      const answer =
        body.question === matrixQuestion
          ? matrixAnswer
          : 'Row reduction preserves the solution set; a pivot in every column confirms invertibility.';
      return answerStream(turns, finishingChunks(turns, answer), ready);
    }),
  ];
}

export function askFixture(state: AskState = 'new', held = false): ReviewFixture & { release: () => void } {
  let gate = completionGate();
  return {
    id: `synthetic-ask-${state}${held ? '-streaming' : ''}-v1`,
    release: () => gate.release(),
    handlers: () => {
      gate = completionGate();
      if (!held) gate.release();
      return askHandlers(state, gate.promise);
    },
  };
}
