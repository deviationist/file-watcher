import { describe, it, expect } from "vitest";
import { loadPublisherConfig, loadSubscriberConfig, resolveWatchPaths, type PublisherOpts } from "./config.js";

const noFlags: PublisherOpts = { watchPaths: [], watchFolders: [], watchExtensions: [], ignorePatterns: [], watchEvents: [] };
const broker = { MQTT_BROKER_URL: "mqtt://localhost:1883" };

describe("resolveWatchPaths — WATCH_PATHS, and the name it used to have", () => {
  it("reads WATCH_PATHS", () => {
    expect(resolveWatchPaths(noFlags, { WATCH_PATHS: "/a, /b/master.db" }))
      .toEqual({ ok: true, paths: ["/a", "/b/master.db"], warnings: [] });
  });

  it("still reads WATCH_FOLDERS, so a host can take the new code before its .env changes — with a warning", () => {
    const r = resolveWatchPaths(noFlags, { WATCH_FOLDERS: "/a" });
    expect(r).toMatchObject({ ok: true, paths: ["/a"] });
    expect(r.ok && r.warnings[0]).toMatch(/WATCH_FOLDERS is deprecated/);
  });

  it("refuses both at once, because two lists that disagree leave nobody sure which is live", () => {
    expect(resolveWatchPaths(noFlags, { WATCH_PATHS: "/a", WATCH_FOLDERS: "/b" }).ok).toBe(false);
  });

  it("prefers the flag over the environment", () => {
    expect(resolveWatchPaths({ ...noFlags, watchPaths: ["/flag"] }, { WATCH_PATHS: "/env" }))
      .toMatchObject({ ok: true, paths: ["/flag"] });
  });

  it("accepts the old flag with a warning, and refuses old and new flags together", () => {
    expect(resolveWatchPaths({ ...noFlags, watchFolders: ["/a"] }, {})).toMatchObject({ ok: true, paths: ["/a"] });
    expect(resolveWatchPaths({ ...noFlags, watchFolders: ["/a"], watchPaths: ["/b"] }, {}).ok).toBe(false);
  });

  it("requires one of them", () => {
    const r = resolveWatchPaths(noFlags, { WATCH_PATHS: " , " });
    expect(r).toEqual({ ok: false, error: expect.stringMatching(/WATCH_PATHS is required/) });
  });
});

describe("loadPublisherConfig", () => {
  it("reads xavi's publisher .env as it stands", () => {
    const r = loadPublisherConfig(noFlags, {
      ...broker,
      WATCH_FOLDERS: "/mnt/music/main,/mnt/music/on-hold/Vurderes,/mnt/music/on-hold/Ingress,/mnt/resilio/data/rekordbox/master.db",
      WATCH_EXTENSIONS: "mp3,aiff,aif,flac,wav,m4a",
      EVENT_SOURCE: "inotify",
    });
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.config.watchPaths).toHaveLength(4);
    expect(r.config.eventSource).toBe("inotify");
    expect([...r.config.watchExtensions]).toEqual(["mp3", "aiff", "aif", "flac", "wav", "m4a"]);
  });

  it("defaults to chokidar polling, the mode for mounts that emit no inotify events", () => {
    const r = loadPublisherConfig(noFlags, { ...broker, WATCH_PATHS: "/a" });
    expect(r.ok && [r.config.eventSource, r.config.usePolling]).toEqual(["chokidar", true]);
  });

  it("lowercases extensions, so AIFF and aiff are one rule", () => {
    const r = loadPublisherConfig(noFlags, { ...broker, WATCH_PATHS: "/a", WATCH_EXTENSIONS: "AIFF,Mp3" });
    expect(r.ok && [...r.config.watchExtensions]).toEqual(["aiff", "mp3"]);
  });

  it("makes the catch-up window twice its interval unless told otherwise", () => {
    const r = loadPublisherConfig(noFlags, { ...broker, WATCH_PATHS: "/a", CATCHUP_INTERVAL_SECONDS: "100" });
    expect(r.ok && r.config.catchUpWindowSeconds).toBe(200);
  });

  it("refuses an unknown event source or stability mode rather than falling back", () => {
    expect(loadPublisherConfig(noFlags, { ...broker, WATCH_PATHS: "/a", EVENT_SOURCE: "fanotify" }).ok).toBe(false);
    expect(loadPublisherConfig(noFlags, { ...broker, WATCH_PATHS: "/a", STABILITY_MODE: "x" }).ok).toBe(false);
  });

  it("requires a broker", () => {
    expect(loadPublisherConfig(noFlags, { WATCH_PATHS: "/a" }).ok).toBe(false);
  });
});

describe("loadSubscriberConfig", () => {
  const subFlags = { pathPrefixes: [] as string[] };

  it("reads the rekordbox unit's flags", () => {
    const r = loadSubscriberConfig({
      ...subFlags, pathPrefixes: ["/mnt/resilio/data/rekordbox"], debounceSeconds: 30,
      onChangeCommand: "poetry run rekordbox2plex playlists",
    }, broker);
    expect(r).toMatchObject({ ok: true, config: { debounceSeconds: 30, pathPrefixes: ["/mnt/resilio/data/rekordbox"] } });
  });

  it("requires a command", () => {
    expect(loadSubscriberConfig(subFlags, broker).ok).toBe(false);
  });
});
