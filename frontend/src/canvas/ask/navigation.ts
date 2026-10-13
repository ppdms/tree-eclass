let newQuestionVersion = 0;

/** Explicit New question links reset even when their URL differs only by its hash. */
export function requestNewQuestion(): void {
  newQuestionVersion += 1;
}

export function currentNewQuestionVersion(): number {
  return newQuestionVersion;
}
