// Shared MOTU datastore poller.
// ONE long-poll loop shared by every monitor button - never one connection per
// button. The datastore HTTP server is fragile under rapid concurrent requests
// (wedge observed 2026-11-06), so all state feedback rides a single request.
import streamDeck from "@elgato/streamdeck";
import { MotuApi } from "./motu-avb-api";

export type Datastore = Record<string, string | number>;
type Listener = (datastore: Datastore) => void;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export class MotuPoller {
    private static instance: MotuPoller;
    private listeners = new Set<Listener>();
    private running = false;
    private etag: string | null = null;
    private snapshot: Datastore = {};

    static getInstance(): MotuPoller {
        if (!MotuPoller.instance) MotuPoller.instance = new MotuPoller();
        return MotuPoller.instance;
    }

    /** Register a listener; fires immediately with the last known snapshot. */
    subscribe(fn: Listener): void {
        this.listeners.add(fn);
        if (Object.keys(this.snapshot).length > 0) {
            try {
                fn(this.snapshot);
            } catch (e) {
                streamDeck.logger.error(`[MotuPoller] listener error: ${e}`);
            }
        }
        this.start();
    }

    unsubscribe(fn: Listener): void {
        this.listeners.delete(fn);
    }

    getSnapshot(): Datastore {
        return this.snapshot;
    }

    /**
     * Immediate plain fetch + notify (no If-None-Match). Used right after the
     * plugin itself PATCHes values so pressed buttons reflect state instantly
     * instead of waiting for the held long-poll to wake.
     */
    async refreshNow(): Promise<void> {
        const url = MotuApi.getInstance().getUrl();
        if (!url) return;
        try {
            const res = await fetch(url);
            if (res.ok) {
                const etag = res.headers.get("etag");
                if (etag) this.etag = etag;
                const data = (await res.json()) as Datastore;
                this.snapshot = data;
                for (const fn of [...this.listeners]) {
                    try {
                        fn(data);
                    } catch (e) {
                        streamDeck.logger.error(`[MotuPoller] listener error: ${e}`);
                    }
                }
            }
        } catch (e) {
            streamDeck.logger.warn(`[MotuPoller] refreshNow error: ${e}`);
        }
    }

    private start(): void {
        if (this.running) return;
        this.running = true;
        void this.loop();
    }

    /**
     * Long-poll the root datastore with If-None-Match. The device holds the
     * request up to ~15s and answers 200 with fresh data on any change, or 304
     * when nothing changed during the hold. Falls back to plain 2s polling if
     * the ETag path misbehaves (errors -> 2s backoff either way).
     */
    private async loop(): Promise<void> {
        while (this.running) {
            const url = MotuApi.getInstance().getUrl();
            if (!url) {
                await sleep(1000);
                continue;
            }
            try {
                const headers: Record<string, string> = {};
                if (this.etag) headers["If-None-Match"] = this.etag;
                const controller = new AbortController();
                const timer = setTimeout(() => controller.abort(), 20000);
                const res = await fetch(url, { headers, signal: controller.signal });
                clearTimeout(timer);

                if (res.ok) {
                    const etag = res.headers.get("etag");
                    if (etag) this.etag = etag;
                    const data = (await res.json()) as Datastore;
                    this.snapshot = data;
                    for (const fn of [...this.listeners]) {
                        try {
                            fn(data);
                        } catch (e) {
                            streamDeck.logger.error(`[MotuPoller] listener error: ${e}`);
                        }
                    }
                    if (!etag) {
                        // server does not support If-None-Match: fall back to 2s polling
                        // instead of refetching in a tight loop (which can wedge it)
                        await sleep(2000);
                    }
                } else if (res.status === 304) {
                    // unchanged during the hold - re-issue immediately
                } else {
                    streamDeck.logger.warn(`[MotuPoller] poll status ${res.status}`);
                    this.etag = null;
                    await sleep(2000);
                }
            } catch (e) {
                streamDeck.logger.warn(`[MotuPoller] poll error: ${e}`);
                this.etag = null;
                await sleep(2000);
            }
        }
    }
}
