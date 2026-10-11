import { afterEach, describe, expect, it, vi } from 'vitest';
import { connectLiveStream } from './liveStream';

class Socket {
    static instances: Socket[] = [];
    onmessage: ((event: { data: string }) => void) | null = null;
    onclose: ((event: { code: number }) => void) | null = null;
    onerror: (() => void) | null = null;
    constructor(public url: string) { Socket.instances.push(this); }
    close = vi.fn(() => this.onclose?.({ code: 1000 }));
    message(data: unknown) { this.onmessage?.({ data: JSON.stringify(data) }); }
}

afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); Socket.instances = []; });

describe('live socket', () => {
    function setup() {
        vi.useFakeTimers();
        vi.stubGlobal('location', { protocol: 'http:', host: 'localhost:8800' });
        vi.stubGlobal('WebSocket', Socket);
        vi.stubGlobal('window', { dispatchEvent: vi.fn() });
        const receive = vi.fn(), failed = vi.fn();
        const stop = connectLiveStream(receive, failed);
        return { receive, failed, stop };
    }

    it('receives pushed updates and stops without reconnecting', () => {
        const { receive, stop } = setup();
        const socket = Socket.instances[0];
        expect(socket.url).toBe('ws://localhost:8800/ws/live-markets');
        const payload = { ts_ms: 1, markets: [], kalshi_connected: true, coinbase_connected: true };
        socket.message(payload);
        expect(receive).toHaveBeenCalledWith(payload);
        stop();
        vi.advanceTimersByTime(30_000);
        expect(Socket.instances).toHaveLength(1);
        socket.message(payload);
        expect(receive).toHaveBeenCalledTimes(1);
    });

    it('retries disconnects with backoff and rejects invalid messages', () => {
        const { receive, failed, stop } = setup();
        Socket.instances[0].message({ markets: [] });
        expect(receive).not.toHaveBeenCalled();
        expect(failed).toHaveBeenCalled();
        vi.advanceTimersByTime(1000);
        expect(Socket.instances).toHaveLength(2);
        Socket.instances[1].close();
        vi.advanceTimersByTime(1999);
        expect(Socket.instances).toHaveLength(2);
        vi.advanceTimersByTime(1);
        expect(Socket.instances).toHaveLength(3);
        stop();
    });

    it('reconnects stalled feeds but never retries an expired login', () => {
        const { stop } = setup();
        vi.advanceTimersByTime(16_000);
        expect(Socket.instances).toHaveLength(2);
        Socket.instances[1].onclose?.({ code: 4401 });
        vi.advanceTimersByTime(30_000);
        expect(Socket.instances).toHaveLength(2);
        expect(window.dispatchEvent).toHaveBeenCalled();
        stop();
    });
});
