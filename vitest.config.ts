import { defineConfig } from "vitest/config";

// `dist/` holds a compiled copy of every test, and vitest picks it up: after a
// build the suite runs each integration test twice, both copies publishing to
// the same broker topic, and each sees the other's messages. It reads as a real
// failure -- "expected 3 events, got 5" -- and is not one.
export default defineConfig({
  test: { exclude: ["dist/**", "node_modules/**"] },
});
