import { wsUrl, type LiveSnapshot } from './api';

export function connectLiveStream(
    receive: (snapshot: LiveSnapshot) => void,
    failed: (message: string) => void,
): () => void {
    let stopped = false;
    let socket: WebSocket | null = null;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let watchdog: ReturnType<typeof setTimeout> | undefined;
    let attempts = 0;
    const open = () => {
        if (stopped) return;
        socket = new WebSocket(wsUrl('/ws/live-markets'));
        const current = socket;
        const armWatchdog = () => {
            clearTimeout(watchdog);
            watchdog = setTimeout(() => {
                failed('The live feed stopped sending updates. Reconnecting.');
                current.close();
            }, 15_000);
        };
        armWatchdog();
        current.onmessage = event => {
            if (stopped || current !== socket) return;
            try {
                const data = JSON.parse(event.data);
                if (data.type === 'error') {
                    failed(data.detail || 'Live feed unavailable.');
                    current.close();
                    return;
                }
                if (!Array.isArray(data.markets) || !Number.isFinite(data.ts_ms) ||
                    typeof data.kalshi_connected !== 'boolean' || typeof data.coinbase_connected !== 'boolean') {
                    throw new Error('Invalid live market update');
                }
                attempts = 0;
                armWatchdog();
                receive(data);
            } catch (error) {
                failed((error as Error).message);
                current.close();
            }
        };
        current.onerror = () => {
            if (!stopped) failed('Live feed connection interrupted. Reconnecting.');
        };
        current.onclose = event => {
            clearTimeout(watchdog);
            if (stopped) return;
            if (event.code === 4401) {
                failed('Your session expired. Sign in again to receive live prices.');
                window.dispatchEvent(new Event('auth:expired'));
                return;
            }
            failed('Live feed disconnected. Reconnecting.');
            retry = setTimeout(open, Math.min(1000 * 2 ** attempts++, 10_000));
        };
    };
    open();
    return () => { stopped = true; clearTimeout(retry); clearTimeout(watchdog); socket?.close(); };
}
