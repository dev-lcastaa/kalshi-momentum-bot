import { describe, expect, it } from 'vitest';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { MarketCard, marketClock, updateLiveHistory } from './LiveMarkets';
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

  describe('live market summary', () => {
    it('shows an accurate countdown, clamps elapsed time, and handles rollover', () => {
      expect(marketClock(0, 900_000, 451_000)).toMatchObject({ label: '07:29', phase: 'active' });
      expect(marketClock(0, 900_000, 451_000).progress).toBeCloseTo(451 / 9);
      expect(marketClock(0, 900_000, 840_000)).toMatchObject({ label: '01:00', remaining: 60 });
      expect(marketClock(0, 900_000, 900_000)).toMatchObject({ label: '00:00', progress: 100, phase: 'ended' });
      expect(marketClock(60_000, 960_000, 0)).toMatchObject({ label: '01:00', progress: 0, phase: 'upcoming' });
      expect(marketClock(null, null, 0)).toMatchObject({ label: 'Waiting', progress: 0 });
      expect(marketClock(900, 0, 0).phase).toBe('waiting');
    });

    function card(ref: string | null, strike: string | null, stale = false) {
      const data = snapshot(451_000);
      const market = { ...data.markets[0], ref, strike, ref_ms: stale ? 1 : 451_000 };
      return renderToStaticMarkup(createElement(MarketCard, {
        coin: { series: 'KXBTC15M', name: 'Bitcoin', symbol: 'BTC' },
        market, snapshot: data, points: [], now: 451_000, failed: false,
      }));
    }

    it('separates the time frame, timer, price, target and distance', () => {
      const html = card('82856.85', '82897.41');
      for (const label of ['Market time frame', 'Time remaining', '07:29', 'Kalshi price', 'Price to beat', 'Below target', '$40.56']) {
        expect(html).toContain(label);
      }
      expect(card('82938', '82897.41')).toContain('Above target');
      expect(card('82897.41', '82897.41')).toContain('At target');
    });

    it('does not label Coinbase as Kalshi or show a stale target comparison', () => {
      expect(card(null, '82897.41')).toContain('Waiting for price');
      expect(card('82856.85', '82897.41', true)).not.toContain('Below target');
      expect(card('82856.85', null)).toContain('Needs a live Kalshi price and target');
    });
  });
  it('bounds chart history and does not add duplicate times', () => {
    let history = {};
    for (let i = 0; i < 1000; i++) history = updateLiveHistory(history, snapshot(i * 1000));
    const result = updateLiveHistory(history, snapshot(999_000));
    expect(result.KXBTC15M.points).toHaveLength(901);
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
  it('retains subsecond samples and bounds high-rate history', () => {
    let history = updateLiveHistory({}, snapshot(1000));
    history = updateLiveHistory(history, snapshot(1050));
    expect(history.KXBTC15M.points.map(p => p.ts)).toEqual([1000, 1050]);
    const point = history.KXBTC15M.points[0];
    history.KXBTC15M.points = Array.from({ length: 18_000 }, (_, i) => ({ ...point, ts: i * 50 }));
    const next = updateLiveHistory(history, snapshot(900_000));
    expect(next.KXBTC15M.points).toHaveLength(18_000);
    expect(next.KXBTC15M.points[0].ts).toBe(50);
    expect(next.KXBTC15M.points[17_999].ts).toBe(900_000);
  });
});
