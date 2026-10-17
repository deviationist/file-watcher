/// Helpers for the tests. Not imported by either binary.

/// Poll until `check` holds. Filesystem events arrive when the kernel and the
/// child process get round to them, so a test waits for the outcome rather
/// than sleeping a guess.
export async function waitFor(check: () => boolean, timeoutMs = 3000, what = "condition"): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs}ms waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 10));
  }
}

/// Give events that should *not* arrive a fair chance to arrive anyway, so an
/// assertion of absence means something.
export const settle = (ms = 250): Promise<void> => new Promise((r) => setTimeout(r, ms));
