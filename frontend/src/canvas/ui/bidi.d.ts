declare module 'bidi-js' {
  export type BidiType =
    | 'L'
    | 'R'
    | 'AL'
    | 'EN'
    | 'AN'
    | 'NSM'
    | 'ON'
    | 'BN'
    | 'B'
    | 'S'
    | 'WS'
    | 'ES'
    | 'ET'
    | 'CS'
    | 'LRI'
    | 'RLI'
    | 'FSI'
    | 'PDI'
    | 'LRE'
    | 'RLE'
    | 'LRO'
    | 'RLO'
    | 'PDF';
  interface BidiParagraph {
    start: number;
    end: number;
    level: number;
  }
  export interface BidiLevels {
    levels: Uint8Array;
    paragraphs: BidiParagraph[];
  }
  export interface Bidi {
    getEmbeddingLevels(text: string, direction?: 'ltr' | 'rtl'): BidiLevels;
    getReorderSegments(text: string, levels: BidiLevels, start?: number, end?: number): [number, number][];
    getBidiCharTypeName(character: string): BidiType;
  }
  export default function bidiFactory(): Bidi;
}
