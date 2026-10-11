import { afterEach, describe, expect, it, vi } from 'vitest';
import { animatePrice, interpolatePrice } from './priceMotion';

afterEach(() => vi.unstubAllGlobals());

describe('price display motion', () => {
    it('interpolates without overshooting or changing endpoints', () => {
        expect(interpolatePrice(100, 110, 0)).toBe(100);
        expect(interpolatePrice(100, 110, .5)).toBe(105);
        expect(interpolatePrice(110, 100, .5)).toBe(105);
        expect(interpolatePrice(100, 110, 2)).toBe(110);
        expect(interpolatePrice(100, 110, -1)).toBe(100);
    });
    it('does not invent prices across missing or invalid samples', () => {
        expect(interpolatePrice(null, 110, .5)).toBe(110);
        expect(interpolatePrice(100, null, .5)).toBeNull();
        expect(interpolatePrice(100, NaN, .5)).toBeNull();
    });
    it('renders intermediate frames, reaches the target and cancels cleanly', () => {
        vi.stubGlobal('window', { matchMedia: () => ({ matches: false }) });
        vi.spyOn(performance, 'now').mockReturnValue(0);
        let callback: FrameRequestCallback = () => {};
        const request = vi.fn((fn: FrameRequestCallback) => { callback = fn; return 7; });
        const cancel = vi.fn();
        vi.stubGlobal('requestAnimationFrame', request);
        vi.stubGlobal('cancelAnimationFrame', cancel);
        const draw = vi.fn();
        const stop = animatePrice(100, draw);
        callback(50); callback(100);
        expect(draw.mock.calls.map(call => call[0])).toEqual([0, .5, 1]);
        expect(request).toHaveBeenCalledTimes(2);
        stop();
        expect(cancel).toHaveBeenCalledWith(7);
        vi.restoreAllMocks();
    });
    it('respects reduced motion without starting an animation', () => {
        vi.stubGlobal('window', { matchMedia: () => ({ matches: true }) });
        const draw = vi.fn();
        animatePrice(700, draw);
        expect(draw).toHaveBeenCalledTimes(1);
        expect(draw).toHaveBeenCalledWith(1);
    });
});
