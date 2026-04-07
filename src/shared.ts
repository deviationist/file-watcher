export interface MqttChangePayload {
  event: string;
  path: string;
  timestamp: string;
}

export function log(label: string, message: string): void {
  console.log(`${new Date().toISOString()} [${label}] ${message}`);
}

export function logError(label: string, message: string): void {
  console.error(`${new Date().toISOString()} [${label}] ERROR: ${message}`);
}

export function parseCommaSeparated(value: string, prev: string[]): string[] {
  return prev.concat(value.split(",").map((s) => s.trim()).filter(Boolean));
}
