import initTaffy from 'taffy-layout/wasm';
import { loadFonts, type Fonts } from '../ui/fonts';
import type { UIRuntime } from '../ui/runtime';
import type { ToApp } from './protocol';

export type ApplicationMessage = Exclude<ToApp, { type: 'fonts' | 'layout' | 'boot' }>;
export type ReceiveApplication = (message: ApplicationMessage) => void;
export interface WorkerStartup {
  fonts: Promise<Fonts>;
  runtime: Promise<typeof UIRuntime>;
  /** Absolute timestamps; converted once against the original page's navigation start. */
  stamps: Record<string, number>;
}

export interface StartupFonts {
  inter: string;
  mono: string;
}

/** Loads the prefetched immutable font URLs and instantiates the main thread's compiled layout module. */
export async function loadWorkerAssets(
  fonts: Promise<StartupFonts>,
  layout: Promise<WebAssembly.Module>,
  mark: (key: string) => void,
): Promise<Fonts> {
  const [loaded] = await Promise.all([
    fonts
      .then(({ inter, mono }) => loadFonts(inter, mono))
      .then((value) => {
        mark('fontsReadyMs');
        return value;
      }),
    layout.then((module) => initTaffy({ module_or_path: module })).then(() => mark('taffyReadyMs')),
  ]);
  return loaded;
}
