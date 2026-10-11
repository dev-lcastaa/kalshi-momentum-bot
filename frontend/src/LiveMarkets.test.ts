import { describe, expect, it } from 'vitest';
import { updateLiveHistory } from './LiveMarkets';
import type { LiveMarket, LiveSnapshot } from './api';

function snapshot(ts: number, ticker = 'BTC-now'): LiveSnapshot {
  const market: LiveMarket = {
    series_ticker: 'KXBTC15M', market_ticker: ticker, open_ms: 0, close_ms: 900_000,
    strike: '62500', status: 'active', yes_bid: '0.45', yes_ask: '0.47', last: '0.46',
    price_ms: ts, quote_ms: ts, ref: null, ref_ms: null, coinbase: '62510', coinbase_ms: ts,
  };
  return { ts_ms: ts, kalshi_connected: true, coinbase_connected: true,
    markets: [market, { ...market, series_ticker: 'KXETH15M', market_ticker: 'ETH-now', coinbase: '3200' }] };
}

describe('live market charts', () => {
  it('keeps both coins separate and resets only the market that rolls over', () => {
    const first = updateLiveHistory({}, snapshot(1000));
    const next = updateLiveHistory(first, snapshot(2000, 'BTC-next'));
    expect(next.KXBTC15M.points).toHaveLength(1);
    expect(next.KXETH15M.points).toHaveLength(2);
    expect(next.KXBTC15M.points[0].coinbase).toBe(62510);
    expect(next.KXETH15M.points[0].coinbase).toBe(3200);
  });
  it('bounds chart history and does not add duplicate times', () => {
    let history = {};
    for (let i = 0; i < 1000; i++) history = updateLiveHistory(history, snapshot(i * 1000));
    const result = updateLiveHistory(history, snapshot(999_000));
    expect(result.KXBTC15M.points).toHaveLength(900);
    expect(result.KXBTC15M.points[result.KXBTC15M.points.length - 1].ts).toBe(999_000);
  });
  it('leaves gaps for disconnected or stale prices', () => {
    const data = snapshot(100_000);
    data.kalshi_connected = false;
    data.markets[0].coinbase_ms = 1;
    const result = updateLiveHistory({}, data);
    expect(result.KXBTC15M.points[0].ref).toBeNull();
    expect(result.KXBTC15M.points[0].coinbase).toBeNull();
  });
});
