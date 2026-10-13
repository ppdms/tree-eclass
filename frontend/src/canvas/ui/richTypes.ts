/** Output of the owned HTML/Markdown parsers, not an external wire format. */
export interface TextRun {
  text: string;
  bold?: boolean;
  italic?: boolean;
  mono?: boolean;
  href?: string;
  title?: string;
  strike?: boolean;
}
export interface RichBlock {
  runs: TextRun[];
  pre: boolean;
}
