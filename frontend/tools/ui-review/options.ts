import type { ScenarioEntry } from './schemas';

export interface CaptureOptions {
  list: boolean;
  only: string[];
  out: string;
}

export function options(args: string[]): CaptureOptions {
  const result: CaptureOptions = { list: false, only: [], out: '' };
  for (let index = 0; index < args.length; index++) {
    const key = args[index];
    if (key === '--list') result.list = true;
    else if (key === '--only' || key === '--out') {
      const value = args[++index];
      if (!value || value.startsWith('--')) throw new Error(`${key} requires a value`);
      if (key === '--only') result.only.push(value);
      else {
        if (result.out) throw new Error('Use --out only once');
        result.out = value;
      }
    } else throw new Error(`Unknown option ${key}. Use --list, --only <ID-prefix>, --out <name>.`);
  }
  if (result.out && !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}$/.test(result.out)) {
    throw new Error('--out must be a simple name (letters, digits, dots, underscores or hyphens; max 80)');
  }
  return result;
}

export function select(entries: ScenarioEntry[], prefixes: string[]) {
  for (const prefix of prefixes) {
    if (!entries.some((entry) => entry.id.startsWith(prefix))) {
      throw new Error(`No scenario matches "${prefix}". Use --list for available IDs.`);
    }
  }
  return entries.filter((entry) => !prefixes.length || prefixes.some((prefix) => entry.id.startsWith(prefix)));
}
