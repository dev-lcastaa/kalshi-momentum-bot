import { formatDecimal } from './helpers';
import { friendlyLabel } from './language';

export function researchMoney(value: unknown): string {
    if (typeof value !== 'string' && typeof value !== 'number') return 'Not available';
    const text = String(value);
    if (!/^-?\d+(\.\d+)?$/.test(text)) return 'Not available';
    const formatted = formatDecimal(text, 2);
    return formatted.startsWith('-') ? `-$${formatted.slice(1)}` : `$${formatted}`;
}

export function researchResult(status: string, valid: boolean, fills: unknown, pnl: unknown): string {
    if (status === 'queued') return 'Waiting to start';
    if (status === 'running') return 'Test in progress';
    if (status === 'failed') return 'Test needs attention';
    if (status === 'canceled') return 'Test stopped';
    if (!valid) return 'Not enough reliable results';
    if (typeof fills !== 'number' || !Number.isFinite(fills) || fills < 0) return 'Trade count unavailable';
    if (fills === 0) return 'No trades made';
    if (typeof pnl !== 'string' && typeof pnl !== 'number') return 'Profit / loss unavailable';
    const amount = Number(pnl);
    if (!Number.isFinite(amount) || String(pnl).trim() === '') return 'Profit / loss unavailable';
    if (amount > 0) return 'Made a profit in this test';
    if (amount < 0) return 'Lost money in this test';
    return 'Broke even in this test';
}

export function researchSetting(key: string, value: unknown): string {
    if (value == null) return 'Not available';
    if (typeof value === 'boolean') return value ? 'Yes' : 'No';
    if (typeof value !== 'string' && typeof value !== 'number') return 'Not available';
    if (key === 'latency_ms') return `${value} milliseconds`;
    if (key === 'fee_multiplier') return `${value} × normal fees`;
    if (key === 'starting_balance' || key === 'fees' || key.endsWith('_pnl') || key.includes('loss') || key.includes('notional')) return researchMoney(value);
    if (['take_profit', 'stop_loss', 'min_entry_price', 'max_entry_price', 'max_spread'].includes(key)) {
        return researchMoney(value);
    }
    if (key.endsWith('_s')) return `${value} seconds`;
    if (key === 'queue_model') {
        return { front: 'First in the queue', back: 'Last in the queue', pro_rata: 'Shared by order size' }[String(value)] || String(value);
    }
    if (key === 'ref_source') return value === 'cf' ? 'CF Benchmarks index' : value === 'coinbase' ? 'Coinbase' : String(value);
    return friendlyLabel(String(value));
}
