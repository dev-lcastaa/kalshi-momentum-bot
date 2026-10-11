import { useEffect, useRef, useState } from 'react';
import { AreaSeries, LineSeries, createChart, type AutoscaleInfo, type IChartApi, type ISeriesApi, type LineData, type Time, type UTCTimestamp, type WhitespaceData } from 'lightweight-charts';
import { Link } from 'react-router-dom';
import { type LiveMarket, type LiveSnapshot } from './api';
import { formatCents } from './helpers';
import { Icon } from './Icon';
import { animatePrice, interpolatePrice } from './priceMotion';
import { connectLiveStream } from './liveStream';

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
      points: [...points.filter(p => p.ts < sample.ts && p.ts >= snapshot.ts_ms - 900_000), sample].slice(-18_000),
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
  const displayed = useRef<{ ticker: string | null; ts: number; coinbase: number | null; ref: number | null } | null>(null);
  const loaded = useRef<{ ticker: string | null; start: number; refreshed: number } | null>(null);
  const lastSample = useRef<Sample | null>(null);
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
      priceLineVisible: false, lastValueVisible: false, crosshairMarkerRadius: 4,
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
    const timestamp = points[points.length - 1]?.ts ?? 0;
    const first = points[0]?.ts ?? 0;
    const rebuild = loaded.current?.ticker !== ticker || first < loaded.current.start ||
      (first !== loaded.current.start && timestamp - loaded.current.refreshed >= 5000);
    function rows(key: 'coinbase' | 'ref'): (LineData<UTCTimestamp> | WhitespaceData<UTCTimestamp>)[] {
      const seconds = new Map<number, Sample>();
      const samples = rebuild ? points : points.slice(-1);
      for (const point of samples) seconds.set(point.ts / 1000, point);
      return [...seconds].sort(([a], [b]) => a - b).map(([second, point]) => {
        const value = point[key], time = second as UTCTimestamp;
        return value !== null && Number.isFinite(value) ? { time, value } : { time };
      });
    }
    const priceRows = rows('coinbase'), referenceRows = rows('ref');
    const latestPrice = priceRows[priceRows.length - 1], latestRef = referenceRows[referenceRows.length - 1];
    const nextPrice = latestPrice && 'value' in latestPrice ? latestPrice.value : null;
    const nextRef = latestRef && 'value' in latestRef ? latestRef.value : null;
    const old = displayed.current?.ticker === ticker && timestamp > displayed.current.ts &&
      timestamp - displayed.current.ts <= 2500 ? displayed.current : null;
    if (rebuild) {
      price.setData(priceRows);
      reference.setData(referenceRows);
      loaded.current = { ticker, start: first, refreshed: timestamp };
    } else {
      const previous = lastSample.current;
      if (previous && previous.ts < timestamp) {
        const time = previous.ts / 1000 as UTCTimestamp;
        price.update(previous.coinbase !== null && Number.isFinite(previous.coinbase)
          ? { time, value: previous.coinbase } : { time });
        reference.update(previous.ref !== null && Number.isFinite(previous.ref)
          ? { time, value: previous.ref } : { time });
      }
      if (latestPrice) price.update(latestPrice);
      if (latestRef) reference.update(latestRef);
    }
    lastSample.current = points[points.length - 1] ?? null;
    if (!fitted.current && points.length > 1) { chart.timeScale().fitContent(); fitted.current = true; }
    else if (fitted.current) chart.timeScale().scrollToRealTime();
    return animatePrice(old ? Math.min(150, timestamp - old.ts) : 0, progress => {
      const coinbase = interpolatePrice(old?.coinbase ?? null, nextPrice, progress);
      const ref = interpolatePrice(old?.ref ?? null, nextRef, progress);
      if (latestPrice && coinbase !== null) price.update({ time: latestPrice.time, value: coinbase });
      if (latestRef && ref !== null) reference.update({ time: latestRef.time, value: ref });
      displayed.current = { ticker, ts: timestamp, coinbase, ref };
    });
  }, [points, ticker]);
  useEffect(() => {
    if (!graph.current || strike === null || !Number.isFinite(Number(strike))) return;
    const series = graph.current.price;
    const line = series.createPriceLine({ price: Number(strike), color: '#efd08e', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: 'To beat' });
    return () => { if (graph.current?.price === series) series.removePriceLine(line); };
  }, [strike, ticker]);
  return <div className="live-price-graph" role="img" aria-label={label} ref={host} />;
}

export function marketClock(open: number | null, close: number | null, now: number) {
  if (open === null || close === null || close <= open) return { label: 'Waiting', remaining: null, progress: 0, phase: 'waiting' };
  const phase = now < open ? 'upcoming' : now >= close ? 'ended' : 'active';
  const remaining = Math.max(0, Math.ceil(((phase === 'upcoming' ? open : close) - now) / 1000));
  return {
    label: phase === 'ended' ? '00:00' : `${Math.floor(remaining / 60).toString().padStart(2, '0')}:${String(remaining % 60).padStart(2, '0')}`,
    remaining, phase, progress: Math.max(0, Math.min(100, (now - open) / (close - open) * 100)),
  };
}

export function MarketCard({ coin, market, snapshot, points, now, failed }: {
  coin: typeof COINS[number]; market: LiveMarket | undefined; snapshot: LiveSnapshot | null;
  points: Sample[]; now: number; failed: boolean;
}) {
  const ended = market?.close_ms != null && now >= market.close_ms;
  const clock = marketClock(market?.open_ms ?? null, market?.close_ms ?? null, now);
  const live = !failed && !!snapshot?.kalshi_connected && !!market?.market_ticker && clock.phase === 'active';
  const noAsk = market?.yes_bid == null ? null : (1 - Number(market.yes_bid)).toFixed(4);
  const referenceFresh = live && market?.ref_ms != null && now - market.ref_ms < 30_000;
  const difference = referenceFresh && market?.ref != null && market.strike != null ? Number(market.ref) - Number(market.strike) : null;
  const direction = difference === null ? 'waiting' : difference > 0 ? 'above' : difference < 0 ? 'below' : 'equal';
  return <section className="panel live-market-card" aria-label={`${coin.name} live market`}>
    <div className="live-market-heading">
      <div className="market-cell"><span className={`coin-badge ${coin.symbol === 'ETH' ? 'eth' : ''}`}>{coin.symbol}</span><div><h2>{coin.name}</h2><small>15-minute prediction market</small></div></div>
      <span className={`tag live-status ${live ? 'good' : ''}`}><i />{live ? 'Live' : ended ? 'Market ended' : clock.phase === 'upcoming' ? 'Opens soon' : market?.market_ticker ? 'Updates paused' : 'Waiting for market'}</span>
    </div>
    <div className="live-session">
      <div className="live-timeframe"><span className="live-label"><Icon name="clock" /> Market time frame</span>
        <strong>{market?.open_ms != null && market.close_ms != null
          ? `${new Date(market.open_ms).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })} – ${new Date(market.close_ms).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })}`
          : 'Looking for the current market...'}</strong><small>Your local time · 15 minutes</small>
      </div>
      <div className={`live-countdown ${clock.phase === 'active' && clock.remaining !== null && clock.remaining <= 60 ? 'closing' : ''}`}>
        <span className="live-label">{clock.phase === 'upcoming' ? 'Starts in' : 'Time remaining'}</span>
        <strong aria-label={`${coin.name} countdown`}>{clock.label}</strong>
        <small>{ended ? 'Waiting for the next market' : clock.phase === 'waiting' ? 'Waiting for market times' : 'minutes : seconds'}</small>
      </div>
      <div className="live-time-track" role="progressbar" aria-label={`${coin.name} market time elapsed`} aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(clock.progress)}><span style={{ width: `${clock.progress}%` }} /></div>
    </div>
    <div className="live-price-summary">
      <div className="live-price-tile"><span className="live-label"><span className="source-dot" /> Kalshi price</span><strong>{dollars(market?.ref)}</strong><small>{age(market?.ref_ms, now)}{!referenceFresh && market?.ref != null ? ' · Not live' : ''}</small></div>
      <div className="live-price-tile live-target"><span className="live-label"><Icon name="target" /> Price to beat</span><strong>{dollars(market?.strike)}</strong><small>The target for this market</small></div>
      <div className={`live-price-tile live-distance ${direction}`}>
        <span className="live-label"><Icon name={direction === 'below' ? 'down' : direction === 'above' ? 'up' : 'target'} /> {direction === 'above' ? 'Above target' : direction === 'below' ? 'Below target' : direction === 'equal' ? 'At target' : 'Distance to target'}</span>
        <strong>{difference === null ? 'Waiting' : dollars(String(Math.abs(difference)))}</strong>
        <small>{difference === null ? 'Needs a live Kalshi price and target' : direction === 'equal' ? 'Kalshi price matches the price to beat' : `${direction === 'above' ? 'Higher' : 'Lower'} than the price to beat right now`}</small>
      </div>
    </div>
    <div className="live-chart-section"><div className="live-graph-controls">
      <h3><Icon name="chart" /> Price movement</h3><span>{live ? 'Live stream connected' : 'Waiting for updates'}</span>
    </div><div className="live-graph-legend"><span className="green">Kalshi</span><span className="blue">Coinbase</span><span className="gold">Price to beat</span></div>
    <PriceGraph points={points} strike={market?.strike ?? null} ticker={market?.market_ticker ?? null} label={`${coin.name} Kalshi and Coinbase live price graph`} />
    </div>
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
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  useEffect(() => {
    let active = true;
    let disconnect: (() => void) | undefined;
    const receive = (data: LiveSnapshot) => {
      if (!active) return;
      setSnapshot(data);
      setHistory(previous => updateLiveHistory(previous, data));
      setNow(Date.now()); setError('');
    };
    if (import.meta.env.VITE_MOCK === '1') {
      import('./mock').then(module => { if (active) disconnect = module.mockLiveStream(receive); })
        .catch(error => { if (active) setError((error as Error).message); });
    } else {
      disconnect = connectLiveStream(receive, message => { if (active) setError(message); });
    }
    return () => { active = false; disconnect?.(); };
  }, []);
  return <>
    <div className="page-title"><div><h1>Live markets</h1><p>Watch Bitcoin and Ethereum together. Prices arrive through a live connection.</p></div><Link className="button" to="/windows"><Icon name="replay" /> Watch past markets</Link></div>
    <p className="live-safety"><Icon name="shield" /> Watching does not place trades or use your money.</p>
    {error && <div className="error-box" role="alert">{error} Last received prices remain below; they are not live until updates return.</div>}
    <div className="live-market-grid">{COINS.map(coin => <MarketCard key={coin.series} coin={coin}
      market={snapshot?.markets.find(m => m.series_ticker === coin.series)} snapshot={snapshot}
      points={history[coin.series]?.points ?? []} now={now} failed={!!error} />)}</div>
  </>;
}
