// Oxlint configuration with the vendored anti-slop plugin (tools/oxlint/anti-slop).
// Run from frontend/ with `bun run lint`.
import { defineConfig } from 'oxlint';

export default defineConfig({
  ignorePatterns: ['.next/**', 'dist/**', 'node_modules/**', 'tools/oxlint/anti-slop/**'],
  jsPlugins: [
    { name: 'anti-slop', specifier: './tools/oxlint/anti-slop/index.ts' },
    { name: 'stylex', specifier: '@stylexjs/eslint-plugin' },
    { name: 'stylistic', specifier: '@stylistic/eslint-plugin' },
  ],
  rules: {
    // StyleX catches invalid declarations at compile time only loosely; keep
    // the authoring rules in Oxlint so invalid properties and merge hazards
    // fail before they reach the bundler.
    'stylex/valid-styles': 'error',
    'stylex/no-unused': 'error',
    'stylex/valid-shorthands': 'warn',
    'stylex/sort-keys': 'warn',
    'stylex/enforce-extension': 'error',
    'stylex/no-conflicting-props': 'error',
    // Structural gates: no file over 400 lines, no function body over 50
    // lines. The Stylistic JS plugin supplies the strict 120-character line
    // limit that Oxlint does not provide as a native rule.
    'max-lines': ['error', { max: 400 }],
    'max-lines-per-function': ['error', { max: 50 }],
    'stylistic/max-len': ['error', { code: 120, tabWidth: 2 }],
    // Vendored anti-slop rules: reject low-evidence TS/JS patterns.
    'anti-slop/no-chained-type-assertions': 'error',
    'anti-slop/no-conditional-empty-object-spread': 'error',
    'anti-slop/no-known-value-widening': 'error',
    'anti-slop/no-module-mocking': 'error',
    'anti-slop/no-object-parameters': 'error',
    'anti-slop/no-reflect-apply': 'error',
    'anti-slop/no-reflect-get': 'error',
    // Type guards are the sanctioned place for typeof narrowing (the rule's
    // documented option); ad hoc typeof checks elsewhere still error.
    'anti-slop/no-runtime-typeof': ['error', { allowInTypeGuards: true }],
    'anti-slop/no-shape-in-symbol-names': 'error',
    'anti-slop/no-unknown-parameters': 'error',
    'anti-slop/no-unknown-returns': 'error',
    'anti-slop/no-unknown-type-aliases': 'error',
    'anti-slop/no-unsafe-dictionary-type': 'error',
    'anti-slop/no-widen-then-assert': 'error',
    'anti-slop/require-safety-comment-for-type-assertion': 'error',
  },
});
