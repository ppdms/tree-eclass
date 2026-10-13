/** Load native editing's exact primary faces before the browser host is imported. */
export async function loadEditorFonts(inter: string, mono: string): Promise<void> {
  const faces = [
    new FontFace('Inter Spike', `url(${JSON.stringify(inter)})`, { weight: '100 900' }),
    new FontFace('Roboto Mono Spike', `url(${JSON.stringify(mono)})`, { weight: '100 700' }),
  ];
  for (const face of faces) document.fonts.add(face);
  await Promise.all(faces.map((face) => face.load()));
}
