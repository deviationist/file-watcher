import { createWriteStream, mkdirSync, type WriteStream } from "node:fs";
import path from "node:path";

export interface MqttChangePayload {
  event: string;
  path: string;
  timestamp: string;
}

// File logger — optional tee target. When configured, every log/logError line is
// also appended to this stream. stdout/stderr still receive the line so journald
// keeps capturing service output.
let fileLogStream: WriteStream | null = null;

export function configureFileLogger(logFile: string | undefined, label: string): void {
  if (!logFile) return;
  try {
    mkdirSync(path.dirname(logFile), { recursive: true });
    fileLogStream = createWriteStream(logFile, { flags: "a" });
    fileLogStream.on("error", (err) => {
      // Don't recurse into logError — go straight to stderr.
      console.error(`${new Date().toISOString()} [${label}] ERROR: log file write failed: ${err.message}`);
    });
    log(label, `File logger enabled: ${logFile}`);
  } catch (err) {
    console.error(`${new Date().toISOString()} [${label}] ERROR: failed to open log file '${logFile}': ${err instanceof Error ? err.message : String(err)}`);
  }
}

export function log(label: string, message: string): void {
  const line = `${new Date().toISOString()} [${label}] ${message}`;
  console.log(line);
  fileLogStream?.write(`${line}\n`);
}

export function logError(label: string, message: string): void {
  const line = `${new Date().toISOString()} [${label}] ERROR: ${message}`;
  console.error(line);
  fileLogStream?.write(`${line}\n`);
}

export function parseCommaSeparated(value: string, prev: string[]): string[] {
  return prev.concat(value.split(",").map((s) => s.trim()).filter(Boolean));
}
