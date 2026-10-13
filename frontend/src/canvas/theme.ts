/** Straight (non-premultiplied) RGBA in 0–1. */
export type RGBA = readonly [number, number, number, number];

/** Appearance-dependent Liquid Glass values; shape optics live in `chrome.ts`. */
export interface GlassMaterial {
  /** Tint mixed over the blurred backdrop, with its base strength. */
  tint: RGBA;
  tintAlpha: number;
  /** Extra tint as the backdrop approaches the text color, preserving legibility. */
  adaptive: number;
  /** Additive light so dark glass still reads as a pane over a black page. */
  lift: number;
  rim: number;
  shade: number;
  shadow: number;
  /** Scroll-edge mix toward the page color and its strength at the viewport top. */
  edgeFade: number;
  edgeStrength: number;
}

export interface Palette {
  dark: boolean;
  page: RGBA;
  glass: GlassMaterial;
}

/** Parses the palette's `#rgb`, `#rrggbb` and `#rrggbbaa` colors. */
export function parseColor(value: string): RGBA {
  const hex = value.startsWith('#') ? value.slice(1) : '';
  const full = hex.length === 3 ? [...hex].map((digit) => digit + digit).join('') : hex;
  if (!/^[\da-f]{6}([\da-f]{2})?$/iu.test(full)) throw new Error(`Unsupported canvas color ${value}`);
  const channel = (index: number) => Number.parseInt(full.slice(index * 2, index * 2 + 2), 16) / 255;
  return [channel(0), channel(1), channel(2), full.length === 8 ? channel(3) : 1];
}

export function palette(isDark: boolean, page: string): Palette {
  const glass: GlassMaterial = isDark
    ? {
        tint: [0, 0, 0, 1],
        tintAlpha: 0.16,
        adaptive: 0.3,
        lift: 0.06,
        rim: 0.45,
        shade: 0.2,
        shadow: 0.2,
        edgeFade: 0.35,
        edgeStrength: 0.9,
      }
    : {
        tint: [1, 1, 1, 1],
        tintAlpha: 0.32,
        adaptive: 0.3,
        lift: 0.02,
        rim: 0.7,
        shade: 0.1,
        shadow: 0.07,
        edgeFade: 0.4,
        edgeStrength: 0.9,
      };
  return { dark: isDark, page: parseColor(page), glass };
}
