import { useEffect, useRef, useState } from 'react';

export function interpolatePrice(from: number | null, to: number | null, progress: number): number | null {
    if (to === null || !Number.isFinite(to)) return null;
    if (from === null || !Number.isFinite(from)) return to;
    return from + (to - from) * Math.max(0, Math.min(1, progress));
}

export function animatePrice(duration: number, draw: (progress: number) => void): () => void {
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches || duration <= 0) {
        draw(1);
        return () => {};
    }
    let frame = 0;
    const start = performance.now();
    const tick = (now: number) => {
        const progress = Math.min(1, (now - start) / duration);
        draw(progress);
        if (progress < 1) frame = requestAnimationFrame(tick);
    };
    draw(0);
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
}

export function useReplayMotion(timestamp: number | null, playing: boolean, speed: number, identity: string) {
    const [display, setDisplay] = useState(timestamp);
    const previous = useRef<{ timestamp: number | null; identity: string }>({ timestamp: null, identity });
    useEffect(() => {
        const old = previous.current;
        previous.current = { timestamp, identity };
        if (timestamp === null || old.timestamp === null || old.identity !== identity || !playing ||
            timestamp < old.timestamp || timestamp - old.timestamp > speed * 250) {
            setDisplay(timestamp);
            return;
        }
        return animatePrice(100, progress => setDisplay(interpolatePrice(old.timestamp, timestamp, progress)));
    }, [timestamp, playing, speed, identity]);
    return display;
}
