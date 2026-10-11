import { describe, expect, it } from 'vitest';
import { mockApi } from './mock';
import { researchMoney, researchResult, researchSetting } from './researchPresentation';

describe('plain-language research results', () => {
    it('distinguishes running, incomplete, no-trade and measured results', () => {
        expect(researchResult('queued', false, 0, '0')).toBe('Waiting to start');
        expect(researchResult('running', false, 5, '1')).toBe('Test in progress');
        expect(researchResult('failed', false, 5, '1')).toBe('Test needs attention');
        expect(researchResult('canceled', false, 5, '1')).toBe('Test stopped');
        expect(researchResult('succeeded', false, 5, '1')).toBe('Not enough reliable results');
        expect(researchResult('succeeded', true, 0, '0')).toBe('No trades made');
        expect(researchResult('succeeded', true, 5, '1')).toBe('Made a profit in this test');
        expect(researchResult('succeeded', true, 5, '-1')).toBe('Lost money in this test');
        expect(researchResult('succeeded', true, 5, '0')).toBe('Broke even in this test');
        expect(researchResult('succeeded', true, undefined, '0')).toBe('Trade count unavailable');
        expect(researchResult('succeeded', true, 5, undefined)).toBe('Profit / loss unavailable');
    });
    it('formats dollars without treating missing metrics as zero', () => {
        expect(researchMoney('12.345')).toBe('$12.35');
        expect(researchMoney('-0.055')).toBe('-$0.06');
        expect(researchMoney(undefined)).toBe('Not available');
        expect(researchMoney('NaN')).toBe('Not available');
    });
    it('translates settings into readable units and choices', () => {
        expect(researchSetting('take_profit', '0.04')).toBe('$0.04');
        expect(researchSetting('fees', '0.42')).toBe('$0.42');
        expect(researchSetting('worst_window_pnl', '-1')).toBe('-$1.00');
        expect(researchSetting('latency_ms', 100)).toBe('100 milliseconds');
        expect(researchSetting('fee_multiplier', 1)).toBe('1 × normal fees');
        expect(researchSetting('queue_model', 'back')).toBe('Last in the queue');
        expect(researchSetting('hold_to_settle', false)).toBe('No');
        expect(researchSetting('max_hold_s', 120)).toBe('120 seconds');
    });
});

describe('research demo surfaces', () => {
    it('provides experiment and proposal arrays without claiming real results', async () => {
        const experiments = await mockApi<{ id: number; name: string }[]>('/api/research/experiments');
        expect(experiments[0].name).toBe('Demo experiment');
        const result = await mockApi<{ trials: { valid_for_comparison: boolean }[] }>('/api/research/experiments/1/results');
        expect(result.trials[0].valid_for_comparison).toBe(false);
        expect(Array.isArray(await mockApi('/api/research/paper-proposals'))).toBe(true);
    });
    it('uses clearly fake tokens and supports revocation', async () => {
        const token = await mockApi<{ id: string; token: string }>('/api/research/tokens', {
            method: 'POST', body: JSON.stringify({ label: 'Demo agent' }),
        });
        expect(token.token).toBe('DEMO-NOT-A-REAL-RESEARCH-TOKEN');
        await mockApi(`/api/research/tokens/${token.id}`, { method: 'DELETE' });
        expect(await mockApi('/api/research/tokens')).toEqual([]);
    });
    it('decides a proposal once and refuses unsupported operations', async () => {
        const result = await mockApi<{ status: string }>('/api/research/paper-proposals/1/approve', { method: 'POST' });
        expect(result.status).toBe('approved');
        await expect(mockApi('/api/research/paper-proposals/1/approve', { method: 'POST' })).rejects.toThrow('already decided');
        await expect(mockApi('/api/research/unknown')).rejects.toThrow('unavailable');
    });
});
