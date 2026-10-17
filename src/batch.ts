/// The subscriber's side: which messages it takes, and how it gathers them into
/// one run of its command.
///
/// Out of subscriber.ts for the same reason the publisher's pipeline is out of
/// publisher.ts -- the tests that stood in for this were copies of the logic
/// written inline in the test file, so they passed whatever the subscriber did.

import type { MqttChangePayload } from "./shared.js";

export function matchesPrefix(filePath: string, prefixes: string[]): boolean {
  if (prefixes.length === 0) return true;
  return prefixes.some((prefix) => filePath.startsWith(prefix));
}

/// A message off the wire, or why it is not one.
export function parseMessage(raw: string): { ok: true; payload: MqttChangePayload } | { ok: false; error: string } {
  let parsed: unknown;
  try { parsed = JSON.parse(raw); } catch {
    return { ok: false, error: `Malformed message, skipping: ${raw.slice(0, 200)}` };
  }
  const p = parsed as Partial<MqttChangePayload> | null;
  if (!p || typeof p.path !== "string" || !p.path || typeof p.event !== "string" || !p.event) {
    return { ok: false, error: "Message missing required fields, skipping" };
  }
  return { ok: true, payload: p as MqttChangePayload };
}

export interface Batch {
  events: MqttChangePayload[];
  /// Deduped, in first-seen order -- the CHANGED_PATHS contract.
  paths: string[];
}

/// Collect events until `debounceMs` passes with none, then hand them over as
/// one batch. Every event resets the clock.
export class Debouncer {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private events: MqttChangePayload[] = [];
  private paths = new Set<string>();

  constructor(private readonly debounceMs: number, private readonly fire: (batch: Batch) => void) {}

  add(payload: MqttChangePayload): number {
    this.events.push(payload);
    this.paths.add(payload.path);
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = setTimeout(() => this.flush(), this.debounceMs);
    return this.events.length;
  }

  pending(): { events: number; paths: number } {
    return { events: this.events.length, paths: this.paths.size };
  }

  /// Drop whatever is waiting without running anything. Returns what was dropped.
  cancel(): { events: number; paths: number } {
    const dropped = this.pending();
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    this.events = [];
    this.paths = new Set();
    return dropped;
  }

  private flush(): void {
    this.timer = null;
    const batch = { events: this.events, paths: [...this.paths] };
    this.events = [];
    this.paths = new Set();
    this.fire(batch);
  }
}

/// The environment a command is run with. CHANGED_PATHS is newline-separated
/// and deduped; CHANGED_EVENTS is every event in arrival order.
export function commandEnv(batch: Batch): { CHANGED_PATHS: string; CHANGED_EVENTS: string } {
  return { CHANGED_PATHS: batch.paths.join("\n"), CHANGED_EVENTS: JSON.stringify(batch.events) };
}

export function summarise(batch: Batch): string {
  const byType = batch.events.reduce<Record<string, number>>((acc, e) => {
    acc[e.event] = (acc[e.event] ?? 0) + 1;
    return acc;
  }, {});
  return Object.entries(byType).map(([k, v]) => `${v} ${k}`).join(", ");
}
