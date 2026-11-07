import {
    action,
    DidReceiveSettingsEvent,
    KeyAction,
    KeyDownEvent,
    SingletonAction,
    WillAppearEvent,
    WillDisappearEvent,
} from "@elgato/streamdeck";
import { MotuApi } from "../motu-avb-api";
import { Datastore, MotuPoller } from "../motu-poller";

/**
 * Settings for RawSetMonitor.
 * targets: newline-separated "endpoint=value" pairs. On press EVERY pair is
 * PATCHed; the button shows state 1 (armed) only while the live datastore
 * matches ALL pairs, so the indicator can never go stale.
 */
type RawSetMonitorSettings = {
    targets?: string;
};

type Pair = { endpoint: string; value: string };

export function parseTargets(targets?: string): Pair[] {
    if (!targets) return [];
    const pairs: Pair[] = [];
    for (const line of targets.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const i = trimmed.indexOf("=");
        if (i <= 0) continue;
        pairs.push({ endpoint: trimmed.slice(0, i).trim(), value: trimmed.slice(i + 1).trim() });
    }
    return pairs;
}

function isArmed(pairs: Pair[], ds: Datastore): boolean {
    return pairs.every((p) => String(ds[p.endpoint] ?? "") === p.value);
}

/**
 * Set one or more MOTU AVB endpoints and monitor the live datastore so the
 * key reflects the ACTUAL routing state (armed / unarmed), not a press count.
 *
 * NOTE: settings come from event payloads / a per-key cache, NEVER from
 * action.getSettings() - that call re-triggers onDidReceiveSettings, which
 * would make every refresh recursively schedule another one.
 */
@action({ UUID: "com.simonedenadai.motu-avb.rawsetmonitor" })
export class RawSetMonitor extends SingletonAction<RawSetMonitorSettings> {
    private motuApi = MotuApi.getInstance();
    private poller = MotuPoller.getInstance();
    private watchers = new Map<string, (ds: Datastore) => void>();
    private settingsCache = new Map<string, RawSetMonitorSettings>();

    override async onKeyDown(ev: KeyDownEvent<RawSetMonitorSettings>): Promise<void> {
        const pairs = parseTargets(ev.payload.settings.targets);
        if (pairs.length === 0) {
            ev.action.showAlert();
            return;
        }
        for (const p of pairs) {
            await this.motuApi.patch(p.endpoint, p.value);
        }
        // don't wait for the long-poll to wake: pull state right now
        void this.poller.refreshNow();
        await this.refresh(ev.action as KeyAction<RawSetMonitorSettings>, ev.payload.settings);
    }

    override async onWillAppear(ev: WillAppearEvent<RawSetMonitorSettings>): Promise<void> {
        const keyAction = ev.action as KeyAction<RawSetMonitorSettings>;
        this.settingsCache.set(keyAction.id, ev.payload.settings);
        await this.refresh(keyAction, ev.payload.settings);
        this.watch(keyAction);
    }

    override onWillDisappear(ev: WillDisappearEvent<RawSetMonitorSettings>): void {
        const fn = this.watchers.get(ev.action.id);
        if (fn) {
            this.poller.unsubscribe(fn);
            this.watchers.delete(ev.action.id);
        }
        this.settingsCache.delete(ev.action.id);
    }

    override async onDidReceiveSettings(ev: DidReceiveSettingsEvent<RawSetMonitorSettings>): Promise<void> {
        const keyAction = ev.action as KeyAction<RawSetMonitorSettings>;
        this.settingsCache.set(keyAction.id, ev.payload.settings);
        await this.refresh(keyAction, ev.payload.settings);
        this.watch(keyAction);
    }

    private watch(action: KeyAction<RawSetMonitorSettings>): void {
        if (this.watchers.has(action.id)) return;
        const fn = () => {
            void this.refresh(action, this.settingsCache.get(action.id));
        };
        this.watchers.set(action.id, fn);
        this.poller.subscribe(fn);
    }

    private async refresh(
        action: KeyAction<RawSetMonitorSettings>,
        settings?: RawSetMonitorSettings,
    ): Promise<void> {
        try {
            if (action.isInMultiAction()) return;
            const pairs = parseTargets((settings ?? this.settingsCache.get(action.id))?.targets);
            if (pairs.length === 0) return;
            action.setState(isArmed(pairs, this.poller.getSnapshot()) ? 1 : 0);
        } catch (e) {
            // poller snapshot may be empty before the first poll; stay silent
        }
    }
}
