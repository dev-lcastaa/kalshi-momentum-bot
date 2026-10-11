import { useEffect, useRef, useState } from 'react';
import { AreaSeries, LineSeries, createChart, type AutoscaleInfo, type IChartApi, type ISeriesApi, type LineData, type Time, type UTCTimestamp, type WhitespaceData } from 'lightweight-charts';
import { Link } from 'react-router-dom';
import { api, type LiveMarket, type LiveSnapshot } from './api';
import { formatCents } from './helpers';
import { Icon } from './Icon';

const COINS = [
  { series: 'KXBTC15M', name: 'Bitcoin', symbol: 'BTC' },
  { series: 'KXETH15M', name: 'Ethereum', symbol: 'ETH' },
] as const;
type Sample = { ts: number; coinbase: number | null; ref: number | null };
export type LiveHistory = Record<string, { ticker: string; points: Sample[] }>;

export function updateLiveHistory(previous: LiveHistory, snapshot: LiveSnapshot): LiveHistory {
  const next: LiveHistory = {};
  for (const market of snapshot.markets) {
    if (!market.market_ticker) continue;
    const old = previous[market.series_ticker];
    const points = old?.ticker === market.market_ticker ? old.points : [];
    const sample = {
      ts: snapshot.ts_ms,
      coinbase: snapshot.coinbase_connected && market.coinbase_ms !== null &&
        snapshot.ts_ms - market.coinbase_ms < 30_000 && market.coinbase !== null ? Number(market.coinbase) : null,
      ref: snapshot.kalshi_connected && market.ref_ms !== null &&
        snapshot.ts_ms - market.ref_ms < 30_000 && market.ref !== null ? Number(market.ref) : null,
    };
    next[market.series_ticker] = {
      ticker: market.market_ticker,
      points: [...points.filter(p => p.ts < sample.ts && p.ts >= snapshot.ts_ms - 900_000), sample].slice(-900),
    };
  }
  return next;
}

function dollars(value: string | null | undefined) {
  return value == null ? 'Waiting for price' : `$${Number(value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function age(timestamp: number | null | undefined, now: number) {
  if (timestamp == null) return 'Waiting for an update';
  const seconds = Math.max(0, Math.floor((now - timestamp) / 1000));
  return seconds < 2 ? 'Updated just now' : `Updated ${seconds}s ago`;
}

function PriceGraph({ points, label, strike, ticker }: {
  points: Sample[]; label: string; strike: string | null; ticker: string | null;
}) {
  const host = useRef<HTMLDivElement>(null);
  const graph = useRef<{ chart: IChartApi; price: ISeriesApi<'Area'>; reference: ISeriesApi<'Line'> } | null>(null);
  const fitted = useRef(false);
  const target = useRef(strike);
  target.current = strike;
  useEffect(() => {
    if (!host.current) return;
    fitted.current = false;
    let removed = false;
    const chart = createChart(host.current, {
      height: 360,
      layout: { background: { color: 'transparent' }, textColor: '#919bb4' },
      grid: { vertLines: { visible: false }, horzLines: { color: '#cbd5ff09' } },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: .15, bottom: .15 } },
      timeScale: {
        borderVisible: false, timeVisible: true, secondsVisible: true,
        tickMarkFormatter: (time: Time) => typeof time === 'number'
          ? new Date(time * 1000).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit', second: '2-digit' }) : '',
      },
      crosshair: { vertLine: { color: '#a4b6df44', labelBackgroundColor: '#30394f' }, horzLine: { color: '#a4b6df44', labelBackgroundColor: '#30394f' } },
      localization: {
        priceFormatter: (value: number) => `$${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
        timeFormatter: (time: Time) => typeof time === 'number' ? new Date(time * 1000).toLocaleTimeString() : '',
      },
    });
    const price = chart.addSeries(AreaSeries, {
      lineColor: '#91b9ff', topColor: '#91b9ff25', bottomColor: '#91b9ff02', lineWidth: 2,
      priceLineVisible: false, lastValueVisible: true, crosshairMarkerRadius: 4,
      autoscaleInfoProvider: (original: () => AutoscaleInfo | null) => {
        const scale = original();
        if (scale?.priceRange && target.current !== null && Number.isFinite(Number(target.current))) {
          scale.priceRange.minValue = Math.min(scale.priceRange.minValue, Number(target.current));
          scale.priceRange.maxValue = Math.max(scale.priceRange.maxValue, Number(target.current));
        }
        return scale;
      },
    });
    const reference = chart.addSeries(LineSeries, {
      color: '#67e4be', lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
    });
    graph.current = { chart, price, reference };
    const observer = new ResizeObserver(entries => {
      if (!removed) {
        const width = entries[0].contentRect.width;
        chart.applyOptions({ width, height: width < 400 ? 280 : 360 });
      }
    });
    observer.observe(host.current);
    return () => { removed = true; observer.disconnect(); graph.current = null; chart.remove(); };
  }, [ticker]);
  useEffect(() => {
    if (!graph.current) return;
    const { chart, price, reference } = graph.current;
    function rows(key: 'coinbase' | 'ref'): (LineData<UTCTimestamp> | WhitespaceData<UTCTimestamp>)[] {
      const seconds = new Map<number, Sample>();
      for (const point of points) seconds.set(Math.floor(point.ts / 1000), point);
      return [...seconds].sort(([a], [b]) => a - b).map(([second, point]) => {
        const value = point[key], time = second as UTCTimestamp;
        return value !== null && Number.isFinite(value) ? { time, value } : { time };
      });
    }
    price.setData(rows('coinbase'));
    reference.setData(rows('ref'));
    if (!fitted.current && points.length > 1) { chart.timeScale().fitContent(); fitted.current = true; }
    else if (fitted.current) chart.timeScale().scrollToRealTime();
  }, [points, ticker]);
  useEffect(() => {
    if (!graph.current || strike === null || !Number.isFinite(Number(strike))) return;
    const series = graph.current.price;
    const line = series.createPriceLine({ price: Number(strike), color: '#efd08e', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: 'To beat' });
    return () => { if (graph.current?.price === series) series.removePriceLine(line); };
  }, [strike, ticker]);
  return <div className="live-price-graph" role="img" aria-label={label} ref={host} />;
}

function MarketCard({ coin, market, snapshot, points, now, failed }: {
  coin: typeof COINS[number]; market: LiveMarket | undefined; snapshot: LiveSnapshot | null;
  points: Sample[]; now: number; failed: boolean;
}) {
  const ended = market?.close_ms != null && now >= market.close_ms;
  const live = !failed && !!snapshot?.kalshi_connected && !!market?.market_ticker && !ended;
  const remaining = market?.close_ms == null ? null : Math.max(0, Math.ceil((market.close_ms - now) / 1000));
  const noAsk = market?.yes_bid == null ? null : (1 - Number(market.yes_bid)).toFixed(4);
  const referenceFresh = live && market?.ref_ms != null && now - market.ref_ms < 30_000;
  const hasReference = market?.ref != null;
  const difference = referenceFresh && market?.ref != null && market.strike != null ? Number(market.ref) - Number(market.strike) : null;
  return <section className="panel live-market-card" aria-label={`${coin.name} live market`}>
    <div className="live-market-heading">
      <div className="market-cell"><span className={`coin-badge ${coin.symbol === 'ETH' ? 'eth' : ''}`}>{coin.symbol}</span><div><h2>{coin.name}</h2><small>15-minute market</small></div></div>
      <span className={`tag ${live ? 'good' : ''}`}>{live ? 'Live' : market?.market_ticker ? 'Updates paused' : 'Waiting for market'}</span>
    </div>
    <div className="live-window">
      <span>{market?.open_ms != null && market.close_ms != null
        ? `${new Date(market.open_ms).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })} – ${new Date(market.close_ms).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })}`
        : 'Looking for the current market...'}</span>
      <b>{remaining === null ? '' : remaining === 0 ? 'Switching to the next market...' : `${Math.floor(remaining / 60)}:${String(remaining % 60).padStart(2, '0')} left`}</b>
    </div>
    <div className="live-price-hero">
      <div><small>{hasReference ? 'Kalshi price' : 'Coinbase price'}</small><strong>{dollars(hasReference ? market?.ref : market?.coinbase)}</strong><small>{age(hasReference ? market?.ref_ms : market?.coinbase_ms, now)}{snapshot && !(hasReference ? snapshot.kalshi_connected : snapshot.coinbase_connected) ? ' · Connection lost' : ''}</small></div>
      <div className="live-target"><small>Price to beat</small><b>{dollars(market?.strike)}</b>
        {difference !== null ? <span className={`live-direction ${difference >= 0 ? 'above' : 'below'}`}>{dollars(String(Math.abs(difference)))} {difference >= 0 ? 'above' : 'below'} the price to beat</span> : <span className="live-direction">Waiting for Kalshi's price</span>}
      </div>
    </div>
    <div className="live-graph-controls">
      <h3>Price movement</h3><span>{live ? 'Updates every second' : 'Waiting for updates'}</span>
    </div>
    <div className="live-graph-legend"><span className="green">Kalshi price</span><span className="blue">Coinbase price</span><span className="gold">Price to beat</span></div>
    <PriceGraph points={points} strike={market?.strike ?? null} ticker={market?.market_ticker ?? null} label={`${coin.name} Kalshi and Coinbase live price graph`} />
    <div className="live-contract-prices">
      <div><small>YES costs</small><strong>{market?.yes_ask == null ? 'Waiting' : formatCents(market.yes_ask)}</strong></div>
      <div><small>NO costs</small><strong>{noAsk === null ? 'Waiting' : formatCents(noAsk)}</strong></div>
      <div><small>Last price</small><strong>{market?.last == null ? 'Waiting' : formatCents(market.last)}</strong></div>
    </div>
    <p className="live-update-note">{age(market?.quote_ms, now)} · Cost per prediction, before fees.</p>
    <details className="live-details"><summary>How to read this market</summary>
      <p>YES wins if Kalshi's average coin price during the final minute finishes at or above the price to beat. Being above or below that price right now does not guarantee the result.</p>
      <p>The green line is the coin price Kalshi watches. The blue line is Coinbase's coin price. They can differ. The dashed gold line is the price to beat.</p>
      <p>Charts start when you open this page. A missing line means we are waiting for that source's price.</p>
      <p>Kalshi price: {dollars(market?.ref)} · {age(market?.ref_ms, now)}</p>
      {market?.market_ticker && <code>{market.market_ticker}</code>}
    </details>
  </section>;
}

export default function LiveMarkets() {
  const [snapshot, setSnapshot] = useState<LiveSnapshot | null>(null);
  const [history, setHistory] = useState<LiveHistory>({});
  const [error, setError] = useState('');
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    let active = true;
    let timer: ReturnType<typeof setTimeout>;
    async function refresh() {
      try {
        const data = await api<LiveSnapshot>('/api/live-markets');
        if (!active) return;
        setSnapshot(data);
        setHistory(previous => updateLiveHistory(previous, data));
        setError('');
      } catch (e) {
        if (active) setError((e as Error).message);
      } finally {
        if (active) { setNow(Date.now()); timer = setTimeout(refresh, 1000); }
      }
    }
    void refresh();
    return () => { active = false; clearTimeout(timer); };
  }, []);
  return <>
    <div className="page-title"><div><h1>Live markets</h1><p>Watch Bitcoin and Ethereum together. Prices refresh every second.</p></div><Link className="button" to="/windows"><Icon name="replay" /> Watch past markets</Link></div>
    <p className="live-safety"><Icon name="shield" /> Watching does not place trades or use your money.</p>
    {error && <div className="error-box" role="alert">{error} Last received prices remain below; they are not live until updates return.</div>}
    <div className="live-market-grid">{COINS.map(coin => <MarketCard key={coin.series} coin={coin}
      market={snapshot?.markets.find(m => m.series_ticker === coin.series)} snapshot={snapshot}
      points={history[coin.series]?.points ?? []} now={now} failed={!!error} />)}</div>
  </>;
}
