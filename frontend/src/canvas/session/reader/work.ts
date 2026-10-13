/** Rejection is not completion: retain the page slot until every sibling job has settled. */
export async function settledPageWork<T extends readonly unknown[]>(work: { [K in keyof T]: Promise<T[K]> }): Promise<{
  -readonly [K in keyof T]: T[K];
}> {
  const results = await Promise.allSettled(work);
  const failed = results.find((result) => result.status === 'rejected');
  if (failed?.status === 'rejected') throw failed.reason;
  const values = results.map((result) => {
    if (result.status === 'rejected') throw result.reason;
    return result.value;
  });
  // SAFETY: allSettled preserves tuple length/order; every result above is fulfilled with its corresponding T[K].
  return values as {
    -readonly [K in keyof T]: T[K];
  };
}
