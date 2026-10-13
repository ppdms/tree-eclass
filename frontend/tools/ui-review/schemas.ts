import { z } from 'zod/v4';

export const entrySchema = z.object({
  id: z.string().regex(/^[a-z0-9-]+$/),
  type: z.enum(['story', 'docs']),
  title: z.string(),
  name: z.string(),
  importPath: z.string(),
  tags: z.array(z.string()),
});
export const storybookIndexSchema = z.object({ entries: z.record(z.string(), entrySchema) });
export const indexSchema = z.object({
  schemaVersion: z.literal(1),
  scenarios: z.array(entrySchema),
});
export type ScenarioEntry = z.infer<typeof entrySchema>;

export const provenanceSchema = z.object({
  schemaVersion: z.literal(1),
  builtAt: z.string(),
  sourceHash: z.string(),
  sources: z.array(z.object({ path: z.string(), sha256: z.string() })),
  artifactHash: z.string(),
  tools: z.object({ storybook: z.string(), playwright: z.string(), msw: z.string() }),
  fixtureSource: z.literal('synthetic'),
});
export type Provenance = z.infer<typeof provenanceSchema>;

export const stateSchema = z.object({
  status: z.enum(['running', 'passed', 'failed']),
  fixture: z.object({ id: z.string(), source: z.literal('synthetic') }),
  requiredCheckpoints: z.array(z.string().regex(/^[a-z0-9-]+$/)),
  checkpoints: z.array(z.string()),
  expectedHttpErrors: z.array(
    z.object({
      method: z.string(),
      path: z.string(),
      status: z.number().int(),
      count: z.number().int().positive(),
    }),
  ),
  expectedNetworkErrors: z.array(
    z.object({
      method: z.string(),
      path: z.string(),
      errorText: z.string(),
      count: z.number().int().positive(),
      requestOrdinal: z.number().int().positive(),
      responseStatus: z.number().int(),
      responseContentType: z.string(),
      responseHeader: z.object({ name: z.string(), value: z.string() }),
      failedCheckpoint: z.string(),
      recoveredCheckpoint: z.string(),
    }),
  ),
  downloads: z.array(
    z.object({
      checkpoint: z.string(),
      filename: z.string(),
      mediaType: z.string(),
      textIncludes: z.string().optional(),
    }),
  ),
  errors: z.array(z.string()),
  theme: z.enum(['dark', 'light', 'eink']),
});
export type ScenarioState = z.infer<typeof stateSchema>;
