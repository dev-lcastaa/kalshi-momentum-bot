import { useCallback, useEffect, useRef, useState } from 'react';
import { Link, Navigate, Route, Routes, useLocation, useNavigate, useParams } from 'react-router-dom';
import { createChart, LineSeries, createSeriesMarkers, type Time } from 'lightweight-charts';
import { formatCents, formatDecimal } from './helpers';
import { api, type Bot, type Frame, type Job, type Overview, type User, type WindowRecord, wsUrl } from './api';
import { Icon, type IconName } from './Icon';
import { APP_NAME, friendlyLabel, marketName, parameterHelp, recordingHelp, outcomeTone } from './language';
import LiveMarkets from './LiveMarkets';
import Research from './Research';
import { useReplayMotion } from './priceMotion';
const money = (value: unknown, digits = 2) => { if (value == null || value === '')
    return 'N/A'; const fixed = formatDecimal(String(value), digits); const [whole, fraction = ''] = fixed.split('.'); return whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',') + (digits ? '.' + fraction : ''); };
const cents = (value: string | null | undefined) => formatCents(value);
const localDate = (value?: string | null) => value ? new Date(value).toLocaleString() : 'N/A';
const utc = (value: number) => new Date(value).toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
type Notice = (text: string) => void;
function Button({ children, onClick, kind = '', disabled = false, type = 'button', title }: {
    children: React.ReactNode;
    onClick?: () => void;
    kind?: string;
    disabled?: boolean;
    type?: 'button' | 'submit';
    title?: string;
}) {
    const icons: Record<string, IconName> = { Play: 'play', Pause: 'pause', Refresh: 'refresh', 'New bot': 'plus', 'Add connection': 'plus', Stop: 'pause', 'Save bot': 'bot', 'Test past markets': 'replay', 'Practice live': 'shield', 'Trade real money': 'chart' };
    const icon = typeof children === 'string' ? icons[children] : undefined;
    return <button type={type} title={title} className={`button ${kind}`} disabled={disabled} onClick={onClick}>{icon && <Icon name={icon} />}{children}</button>;
}
function Panel({ title, children, action }: {
    title: string;
    children: React.ReactNode;
    action?: React.ReactNode;
}) { return <section className="panel"><div className="panel-head"><h2>{title}</h2>{action}</div>{children}</section>; }
function Tag({ children, tone = '' }: {
    children: React.ReactNode;
    tone?: string;
}) { return <span className={`tag ${tone}`}>{typeof children === 'string' ? friendlyLabel(children) : children}</span>; }
function Busy() { return <div className="loading"><i />Loading data...</div>; }
function Empty({ children = 'No records found' }: {
    children?: React.ReactNode;
}) { return <div className="empty">{children}</div>; }
function Table({ columns, rows, empty }: {
    columns: string[];
    rows: React.ReactNode[][];
    empty?: string;
}) { return <div className="table-wrap">{rows.length ? <table><thead><tr>{columns.map((c, i) => <th key={i}>{friendlyLabel(c)}</th>)}</tr></thead><tbody>{rows.map((row, i) => <tr key={i}>{row.map((cell, j) => <td key={j}>{cell}</td>)}</tr>)}</tbody></table> : <Empty>{empty || 'Nothing here yet.'}</Empty>}</div>; }
function PageTitle({ title, subtitle, action }: {
    title: string;
    subtitle: string;
    action?: React.ReactNode;
}) { return <div className="page-title"><div><h1>{title}</h1><p>{subtitle}</p></div>{action}</div>; }
function Modal({ title, children, close }: {
    title: string;
    children: React.ReactNode;
    close: () => void;
}) { return <div className="modal-backdrop" onMouseDown={e => { if (e.target === e.currentTarget)
    close(); }}><div className="modal" role="dialog" aria-modal="true" aria-label={title}><div className="panel-head"><h2>{title}</h2><button onClick={close} aria-label="Close"><Icon name="close" /></button></div>{children}</div></div>; }
function Login({ onLogin }: {
    onLogin: (user: User, csrf: string) => void;
}) { const [username, setUsername] = useState(''), [password, setPassword] = useState(''), [error, setError] = useState(''), [busy, setBusy] = useState(false); const submit = async (e: React.FormEvent) => { e.preventDefault(); setBusy(true); setError(''); try {
    const r = await api<{
        user: User;
        csrf: string;
    }>('/api/auth/login', { method: 'POST', body: JSON.stringify({ username, password }) });
    onLogin(r.user, r.csrf);
}
catch (e) {
    setError((e as Error).message);
}
finally {
    setBusy(false);
} }; return <div className="login-screen"><form className="login-card" onSubmit={submit}><div className="brand-mark"><Icon name="sparkles" /></div><div className="eyebrow">AQLab</div><h1>{APP_NAME}</h1><p>Watch past markets. Test your ideas. Stay in control.</p><h2>Welcome back</h2>{error && <div className="error-box" role="alert">{error}</div>}<label>Username<input autoComplete="username" value={username} onChange={e => setUsername(e.target.value)} required/></label><label>Password<input type="password" autoComplete="current-password" value={password} onChange={e => setPassword(e.target.value)} required/></label><Button type="submit" kind="primary" disabled={busy}>{busy ? 'Signing in...' : 'Sign in'}</Button><small>Practice first. Real-money trading carries a risk of loss.</small></form></div>; }
function Windows() {
    const [items, setItems] = useState<WindowRecord[]>([]), [loading, setLoading] = useState(true);
    const [visible, setVisible] = useState(6), [hasMore, setHasMore] = useState(false), [refresh, setRefresh] = useState(0);
    const [series, setSeries] = useState(''), [from, setFrom] = useState(''), [to, setTo] = useState(''), [error, setError] = useState('');
    useEffect(() => {
        let active = true;
        setLoading(true); setError('');
        const q = new URLSearchParams({ limit: String(Math.min(visible + 1, 1000)) });
        if (series) q.set('series', series);
        if (from) q.set('from', new Date(from).toISOString());
        if (to) q.set('to', new Date(to).toISOString());
        api<WindowRecord[]>(`/api/windows?${q}`).then(rows => {
            if (active) { setItems(rows.slice(0, visible)); setHasMore(rows.length > visible); }
        }).catch(e => { if (active) setError((e as Error).message); }).finally(() => { if (active) setLoading(false); });
        return () => { active = false; };
    }, [series, from, to, visible, refresh]);
    return <>
        <PageTitle title="Market replays" subtitle="Watch a 15-minute market again, at your own pace." />
        <section className="welcome-card">
            <div className="welcome-copy"><span className="eyebrow"><Icon name="sparkles" /> Learn before you trade</span><h2>A rewind button for the market.</h2><p>See how Bitcoin and Ethereum prices moved. Pause, rewind, and try out ideas without spending real money.</p><div className="welcome-tags"><span><Icon name="replay" /> 15-minute recordings</span><span><Icon name="shield" /> No real-money orders</span></div></div>
            <div className="welcome-art" aria-hidden="true"><Icon name="chart" /><span>WATCH. LEARN. REPEAT.</span></div>
        </section>
        <div className="summary-strip">
            <div><span className="summary-icon"><Icon name="replay" /></span><div><b>{loading ? '…' : items.length}</b><small>Recordings shown</small></div></div>
            <div><span className="summary-icon purple"><Icon name="chart" /></span><div><b>Bitcoin & Ethereum</b><small>15-minute prediction markets</small></div></div>
            <div><span className="summary-icon green"><Icon name="shield" /></span><div><b>Watch without trading</b><small>Replays do not place orders</small></div></div>
        </div>
        <Panel title="Choose a recording" action={<Button onClick={() => { setVisible(6); setRefresh(n => n + 1); }}>Refresh</Button>}>
            <div className="filters"><label>Coin<select value={series} onChange={e => { setSeries(e.target.value); setVisible(6); }}><option value="">All coins</option><option value="KXBTC15M">Bitcoin</option><option value="KXETH15M">Ethereum</option></select></label><label>From date<input type="date" value={from} onChange={e => { setFrom(e.target.value); setVisible(6); }} /></label><label>To date<input type="date" value={to} onChange={e => { setTo(e.target.value); setVisible(6); }} /></label><span className="filter-help">Newest first · Times shown in your local timezone.</span></div>
            <div className="recordings-table">{error ? <div className="error-box" role="alert">{error}</div> : loading ? <Busy /> : <Table columns={['Market', 'Starts', 'Target price', 'Outcome', 'Recording', '']} rows={items.map(w => [
                <div className="market-cell"><span className={`coin-badge ${marketName(w.series_ticker) === 'Ethereum' ? 'eth' : ''}`}>{marketName(w.series_ticker) === 'Bitcoin' ? '₿' : 'Ξ'}</span><div><b>{marketName(w.series_ticker)}</b><small>15-minute market</small></div></div>,
                new Date(w.open_time).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }), `$${money(w.strike)}`,
                <Tag tone={outcomeTone(w.result)}>{w.result ? `${w.result.toUpperCase()} won` : 'Not settled yet'}</Tag>,
                <div className="recording-cell"><Tag tone={w.quality}>{w.quality}</Tag><small>{recordingHelp(w.quality)}</small></div>,
                <Link className="button replay-link" to={`/replay/${w.window_id}`}><Icon name="play" /> Watch replay</Link>
            ])} empty="No recordings match these dates. Try another coin or date range." />}</div>
            {!loading && !error && hasMore && <div className="recording-pagination"><Button onClick={() => setVisible(n => Math.min(n + 6, 1000))}>Load more</Button><small>Show the next 6 recordings</small></div>}
            {!loading && !error && visible === 1000 && items.length === 1000 && <p className="panel-help">Showing 1,000 recordings. Choose a date range to find older markets.</p>}
        </Panel>
        <p className="page-note"><Icon name="shield" /> A replay shows the past, not a promise of future profit.</p>
    </>;
}
function Chart({ data, field, markers = [], strike, trades = [], onSeek, playhead }: {
    data: Overview['series'];
    field: 'contract' | 'ref';
    markers?: any[];
    strike?: number;
    trades?: Overview['trades'];
    onSeek?: (ts: number) => void;
    playhead?: number;
}) { const host = useRef<HTMLDivElement>(null), seek = useRef(onSeek), markerPlugin = useRef<any>(null);
const chartRef = useRef<ReturnType<typeof createChart> | null>(null), cursor = useRef<HTMLDivElement>(null);
seek.current = onSeek; useEffect(() => { if (!host.current)
    return; const chart = createChart(host.current, { height: 245, layout: { background: { color: '#10171d' }, textColor: '#84949f' }, grid: { vertLines: { color: '#202c34' }, horzLines: { color: '#202c34' } }, rightPriceScale: { borderColor: '#26333c' }, timeScale: { borderColor: '#26333c', timeVisible: true }, crosshair: { vertLine: { color: '#56717f' }, horzLine: { color: '#56717f' } } }); const series: any[] = []; if (field === 'contract') {
    series.push([chart.addSeries(LineSeries, { color: '#58a9ff', lineWidth: 1, priceLineVisible: false }), 'yes_bid']);
    series.push([chart.addSeries(LineSeries, { color: '#ef8b80', lineWidth: 1, priceLineVisible: false }), 'yes_ask']);
    series.push([chart.addSeries(LineSeries, { color: '#54dbb1', lineWidth: 2, priceLineVisible: false }), 'mid']);
}
else {
    series.push([chart.addSeries(LineSeries, { color: '#58a9ff', lineWidth: 2, priceLineVisible: false }), 'ref']);
    series.push([chart.addSeries(LineSeries, { color: '#efb15b', lineWidth: 1, priceLineVisible: false }), 'coinbase']);
} chartRef.current = chart; for (const [line, key] of series) {
    line.setData(data.filter(p => (p as any)[key] != null).map(p => ({ time: Math.floor(p.ts_ms / 1000) as Time, value: Number((p as any)[key]) })));
    if (key === 'mid')
        markerPlugin.current = createSeriesMarkers(line, []);
    if (key === 'ref' && strike !== undefined)
        line.createPriceLine({ price: strike, color: '#e6b95c', lineWidth: 1, lineStyle: 2, title: 'Target' });
} chart.timeScale().fitContent(); chart.subscribeClick(p => { if (typeof p.time === 'number')
    seek.current?.(p.time * 1000); }); const resize = new ResizeObserver(e => chart.applyOptions({ width: e[0].contentRect.width })); resize.observe(host.current); return () => { resize.disconnect(); chartRef.current = null; chart.remove(); }; }, [data, field, strike]);
useEffect(() => {
    const chart = chartRef.current, element = cursor.current;
    if (!chart || !element || playhead === undefined) return;
    const update = () => {
        const second = Math.floor(playhead / 1000), scale = chart.timeScale();
        const left = scale.timeToCoordinate(second as Time), right = scale.timeToCoordinate((second + 1) as Time);
        const x = left === null ? null : Number(left) + (right === null ? 0 : Number(right) - Number(left)) * (playhead / 1000 - second);
        element.style.display = x === null ? 'none' : 'block';
        if (x !== null) element.style.transform = `translateX(${x}px)`;
    };
    update(); chart.timeScale().subscribeVisibleLogicalRangeChange(update);
    return () => chart.timeScale().unsubscribeVisibleLogicalRangeChange(update);
}, [playhead, data, field, strike]);
useEffect(() => { if (field !== 'contract' || !markerPlugin.current)
    return; const all = [...markers, ...(trades || []).map(t => ({ time: Math.floor(t.ts_ms / 1000), position: t.taker_side === 'yes' ? 'belowBar' : 'aboveBar', color: t.taker_side === 'yes' ? '#49d3a5' : '#ef786f', shape: t.taker_side === 'yes' ? 'arrowUp' : 'arrowDown', text: t.count }))].sort((a, b) => Number(a.time) - Number(b.time)); markerPlugin.current.setMarkers(all); }, [markers, trades, field]); return <div className="replay-chart-wrap"><div className="chart" ref={host}/>{playhead !== undefined && <div className="replay-chart-cursor" ref={cursor} aria-hidden="true" />}</div>; }
function DepthCanvas({ overview, playhead }: {
    overview: Overview;
    playhead: number;
}) { const ref = useRef<HTMLCanvasElement>(null); useEffect(() => { const canvas = ref.current; if (!canvas)
    return; const ctx = canvas.getContext('2d'); if (!ctx)
    return; const ratio = devicePixelRatio || 1, w = canvas.clientWidth, h = 180; canvas.width = w * ratio; canvas.height = h * ratio; ctx.scale(ratio, ratio); ctx.fillStyle = '#10171d'; ctx.fillRect(0, 0, w, h); const span = overview.end_ms - overview.start_ms || 1; for (const bucket of overview.depth) {
    for (const [price, size] of [...bucket.bids, ...bucket.asks]) {
        const x = (bucket.ts_ms - overview.start_ms) / span * w, y = (1 - Number(price)) * h, intensity = Math.min(.85, Math.log1p(Number(size)) / 9);
        ctx.fillStyle = Number(price) < .5 ? `rgba(55,190,146,${intensity})` : `rgba(232,119,93,${intensity})`;
        ctx.fillRect(x, y, Math.max(2, w / Math.max(1, overview.depth.length)), 3);
    }
} }, [overview]); return <div className="replay-chart-wrap"><canvas className="heatmap" ref={ref}/><div className="replay-depth-cursor" aria-hidden="true" style={{ left: `${Math.max(0, Math.min(100, (playhead - overview.start_ms) / (overview.end_ms - overview.start_ms || 1) * 100))}%` }} /></div>; }
function Replay() { const { windowId = '' } = useParams(), [overview, setOverview] = useState<Overview | null>(null), [frame, setFrame] = useState<Frame | null>(null), [events, setEvents] = useState<any[]>([]), [tape, setTape] = useState<any[]>([]), [playing, setPlaying] = useState(false), [speed, setSpeed] = useState(1), [error, setError] = useState(''); const socket = useRef<any>(null); const accept = (f: Frame) => { setFrame(f); setPlaying(f.playing); setSpeed(f.speed); if (f.trades?.length)
    setTape(old => { const all = [...f.trades, ...old]; return all.filter((t, i) => all.findIndex(x => x.ts_ms === t.ts_ms && x.price === t.price && x.taker_side === t.taker_side) === i).slice(0, 500); }); if (f.events?.length)
    setEvents(old => [...f.events, ...old].slice(0, 100)); }; useEffect(() => { let alive = true; api<Overview>(`/api/windows/${encodeURIComponent(windowId)}/overview`).then(v => { if (alive)
    setOverview(v); }).catch(e => setError((e as Error).message)); if (import.meta.env.VITE_MOCK === '1') {
    import('./mock').then(m => { if (alive)
        socket.current = m.mockReplay(accept); });
}
else {
    const ws = new WebSocket(wsUrl(`/ws/replay/${encodeURIComponent(windowId)}`));
    socket.current = ws;
    ws.onmessage = e => { const msg = JSON.parse(e.data); if (msg.type === 'frame')
        accept(msg); if (msg.type === 'error')
        setError(msg.detail); };
    ws.onerror = () => setError('Replay stream connection failed');
} return () => { alive = false; socket.current?.close(); socket.current = null; }; }, [windowId]); const send = (op: string, extra: Record<string, any> = {}) => socket.current?.send(JSON.stringify({ op, ...extra })); useEffect(() => { const key = (e: KeyboardEvent) => { if ((e.target as HTMLElement)?.tagName === 'INPUT' || (e.target as HTMLElement)?.tagName === 'SELECT')
    return; if (e.code === 'Space') {
    e.preventDefault();
    send(playing ? 'pause' : 'play');
} if (e.key === 'ArrowLeft')
    send('step', { ms: -1000 }); if (e.key === 'ArrowRight')
    send('step', { ms: 1000 }); }; window.addEventListener('keydown', key); return () => window.removeEventListener('keydown', key); }, [playing]);
    const smoothTime = useReplayMotion(frame?.ts_ms ?? null, playing, speed, windowId);
    if (!overview)
    return error ? <div className="error-box">{error}</div> : <Busy />;
    const now = frame?.ts_ms ?? overview.start_ms, book = frame?.book, bids = book?.yes_bids || [], asks = book?.yes_asks || [];
    const bid5 = bids.slice(0, 5).reduce((a, b) => a + Number(b[1]), 0), ask5 = asks.slice(0, 5).reduce((a, b) => a + Number(b[1]), 0);
    const imb = bid5 + ask5 ? (bid5 - ask5) / (bid5 + ask5) : 0, trades = tape.filter(t => t.ts_ms <= now);
    const velocity = trades.filter(t => t.ts_ms >= now - 10000).reduce((a, t) => a + Number(t.count), 0), ref = Number(frame?.ref || 0), strike = Number(overview.window.strike);
    const markers = [{ time: Math.floor(overview.open_ms / 1000), position: 'belowBar', color: '#58a9ff', shape: 'arrowUp', text: 'START' }, { time: Math.floor(overview.close_ms / 1000), position: 'aboveBar', color: '#ef786f', shape: 'arrowDown', text: 'END' }, { time: Math.floor(now / 1000), position: 'aboveBar', color: '#e9bd61', shape: 'circle', text: 'YOU ARE HERE' }].filter(x => x.time >= overview.start_ms / 1000 && x.time <= overview.end_ms / 1000);
    const sendSeek = (ts: number) => send('seek', { ts_ms: ts });
    return <>
        <PageTitle title={`${marketName(overview.window.series_ticker)} replay`} subtitle={`15-minute market · Target $${money(overview.window.strike)} · ${localDate(overview.window.open_time)}`} action={<Link className="button" to="/windows"><Icon name="back" /> All recordings</Link>} />
        {error && <div className="error-box" role="alert">{error}</div>}
        <p className="intro-note">Press play to watch this market again. Click a chart or drag the timeline to jump to a moment. Nothing here places real orders.</p>
        <div className="replay-layout"><div className="replay-main">
            <Panel title="YES contract price">
                <p className="panel-help">A YES contract pays $1 if YES wins. These lines show what people offered to buy and sell it for.</p>
                <Chart data={overview.series} field="contract" markers={markers.filter(marker => marker.text !== 'YOU ARE HERE')} trades={overview.trades} onSeek={sendSeek} playhead={smoothTime ?? now} />
                <div className="legend"><span><i className="legend-bid" /> Buy offers</span><span><i className="legend-ask" /> Sell offers</span><span><i /> Price in between</span><span className="muted">Click to jump in time</span></div>
            </Panel>
            <Panel title="Coin price and target">
                <p className="panel-help">Compare the coin price with the target this market is asking about.</p>
                <Chart data={overview.series} field="ref" strike={strike} onSeek={sendSeek} playhead={smoothTime ?? now} />
                <div className="legend"><span><i className="legend-bid" /> Market's reference price (CF Benchmarks)</span><span><i className="legend-coinbase" /> Coinbase: {frame?.coinbase ? `$${money(frame.coinbase)}` : 'Not available'}</span></div>
            </Panel>
            <details className="advanced-panel"><summary>More detail: buy and sell activity</summary>
                <Panel title="Offers at each price"><p className="panel-help">Brighter areas mean more contracts were offered at that price.</p><DepthCanvas overview={overview} playhead={smoothTime ?? now} /><div className="heat-label"><span>$1.00</span><span>YES contract price</span><span>$0.00</span></div></Panel>
                <Panel title="Market updates"><Table columns={['Time', 'Update type', 'What happened']} rows={[...events.map(e => [utc(e.ts_ms), <Tag tone={e.kind}>{e.kind}</Tag>, e.text]), ...overview.lifecycle.map(e => [utc(e.ts_ms), <Tag tone="lifecycle">Market update</Tag>, e.text])]} empty="No market or connection updates to show." /></Panel>
                <p className="panel-help">Market reference: <code>{overview.window.market_ticker}</code></p>
            </details>
        </div><aside className="replay-side">
            <Panel title="At this moment"><div className="metric-grid">
                <Metric label="Best buy offer" value={cents(bids[0]?.[0])} />
                <Metric label="Best sell offer" value={cents(asks[0]?.[0])} />
                <Metric label="Price gap" value={cents(book?.spread)} />
                <Metric label="Price in between" value={cents(book?.mid)} />
                <Metric label="Time left" value={`${Math.max(0, Math.round((overview.close_ms - now) / 1000))}s`} />
                <Metric label="Coin price minus target" value={ref ? `$${money(ref - strike)}` : 'Not available'} />
            </div><p className="panel-help">The price gap is the difference between the best buy and sell offers.</p></Panel>
            <Panel title="Recent trades"><Table columns={['Time', 'Price', 'Contracts', 'Prediction']} rows={trades.slice().reverse().map(t => [new Date(t.ts_ms).toLocaleTimeString(), cents(t.price), t.count, <Tag>{t.taker_side.toUpperCase()}</Tag>])} empty="No trades at this moment yet." /></Panel>
            <details className="advanced-panel"><summary>More detail: waiting offers</summary>
                <Panel title="Waiting buy and sell offers"><BookSide title="Buy YES offers" entries={bids} tone="bid" /><BookSide title="Sell YES offers" entries={asks} tone="ask" /></Panel>
                <Panel title="Trading activity"><div className="metric-grid"><Metric label="Buy / sell balance · top 5" value={`${(imb * 100).toFixed(1)}%`} /><Metric label="Contracts traded · last 10s" value={money(velocity, 0)} /></div><div className="kv"><span>Last traded price</span><b>{cents(frame?.ticker?.price)}</b><span>Contracts traded</span><b>{frame?.ticker?.volume ?? 'Not available'}</b><span>Contracts still held</span><b>{frame?.ticker?.open_interest ?? 'Not available'}</b></div></Panel>
            </details>
        </aside></div>
        <div className="transport" aria-label="Replay controls"><div className="transport-actions">
            <Button kind="primary" onClick={() => send(playing ? 'pause' : 'play')}>{playing ? 'Pause' : 'Play'}</Button>
            <Button title="Go back one second" onClick={() => send('step', { ms: -1000 })}><Icon name="back" /> 1s</Button>
            <Button title="Go forward one second" onClick={() => send('step', { ms: 1000 })}><Icon name="forward" /> 1s</Button>
            <Button title="Go forward ten seconds" onClick={() => send('step', { ms: 10000 })}><Icon name="forward" /> 10s</Button>
            <select aria-label="Playback speed" value={speed} onChange={e => { const n = Number(e.target.value); setSpeed(n); send('speed', { speed: n }); }}>{[.25, .5, 1, 2, 4, 8, 16, 60].map(n => <option value={n} key={n}>{n}x speed</option>)}</select>
        </div><div className="seek-wrap"><input aria-label="Replay timeline" className="seek" type="range" min={overview.start_ms} max={overview.end_ms} value={Math.max(overview.start_ms, Math.min(smoothTime ?? now, overview.end_ms))} onChange={e => sendSeek(Number(e.target.value))} /><div className="slider-markers">{[...overview.lifecycle, ...events].map((ev, i) => <i key={i} className={ev.kind || 'lifecycle'} style={{ left: `${Math.max(0, Math.min(100, (ev.ts_ms - overview.start_ms) / (overview.end_ms - overview.start_ms) * 100))}%` }} title={ev.text} />)}</div></div>
        <div className="transport-time"><span>{new Date(now).toLocaleTimeString()}</span><span>{Math.max(0, Math.round((overview.close_ms - now) / 1000))}s left</span></div></div>
    </>;
}
function Metric({ label, value, tone = '' }: {
    label: string;
    value: string;
    tone?: string;
}) { return <div className="metric"><span>{label}</span><b className={tone}>{value}</b></div>; }
function BookSide({ title, entries, tone }: {
    title: string;
    entries: [
        string,
        string
    ][];
    tone: string;
}) { const max = Math.max(1, ...entries.map(x => Number(x[1]))); return <div className={`book-side ${tone}`}><div className="book-heading"><b>{title}</b><small>Price / contracts</small></div>{entries.length ? entries.slice(0, 10).map((x, i) => <div className="book-row" key={i}><i style={{ width: `${Math.min(100, Number(x[1]) / max * 100)}%` }} /><b>{cents(x[0])}</b><span>{x[1]}</span></div>) : <Empty>No offers available at this moment.</Empty>}</div>; }
function Runs() {
    const [runs, setRuns] = useState<any[]>([]), [loading, setLoading] = useState(true), [error, setError] = useState('');
    useEffect(() => { api<any[]>('/api/runs?limit=50').then(setRuns).catch(e => setError((e as Error).message)).finally(() => setLoading(false)); }, []);
    return <><PageTitle title="Results" subtitle="See what your bot did, and how much it made or lost." />
        <p className="intro-note">Past-market tests and live practice use pretend money. Only results marked “Real-money trading” involve real orders.</p>
        <Panel title="Your recent results">{error ? <div className="error-box">{error}</div> : loading ? <Busy /> : <Table columns={['Result', 'Trading mode', 'Trading approach', 'Status', 'Started', 'Profit / loss', '']} rows={runs.map(r => [<span>#{r.id}</span>, <Tag>{r.mode}</Tag>, friendlyLabel(r.strategy), <Tag tone={r.status === 'succeeded' ? 'good' : r.status === 'failed' ? 'bad' : ''}>{r.status}</Tag>, localDate(r.started_at), `$${money(r.summary?.pnl)}`, <Link className="text-link" to={`/runs/${r.id}`}>View result <Icon name="arrow" /></Link>])} empty="No results yet. Start a past-market test from My bots." />}</Panel></>;
}
function RunDetail() { const { id = '' } = useParams(), [run, setRun] = useState<any>(null), [error, setError] = useState(''), [calib, setCalib] = useState<any>(null), [calibError, setCalibError] = useState(''); const calibrate = async () => { setCalibError(''); try {
    setCalib(await api<any>(`/api/runs/${id}/calibrate`, { method: 'POST', body: JSON.stringify({}) }));
}
catch (e) {
    setCalibError((e as Error).message);
} }; useEffect(() => { api<any>(`/api/runs/${id}`).then(setRun).catch(e => setError((e as Error).message)); }, [id]); if (error)
    return <div className="error-box">{error}</div>; if (!run)
    return <Busy />;
    const oneWindow = run.windows?.length === 1;
    return <><PageTitle title={`Result #${run.id}`} subtitle={`${friendlyLabel(run.mode)} · ${friendlyLabel(run.strategy)} · ${localDate(run.started_at)}`} action={<>{run.mode === 'live' && (calib ? <Link className="button" to="/bots">Accuracy check #{calib.id} is waiting</Link> : <button className="button" onClick={calibrate}><Icon name="shield" /> Check replay accuracy</button>)}{oneWindow && <Link className="button primary" to={`/replay/${run.windows[0]}`}><Icon name="replay" /> Watch this market</Link>}</>} />
        {calibError && <div className="error-box">{calibError}</div>}
        <div className="stat-cards">{['pnl', 'fees', 'n_orders', 'n_fills'].map(k => <div className="stat-card" key={k}><small>{friendlyLabel(k)}</small><b>{run.summary?.[k] == null ? 'Not available' : `${['pnl', 'fees'].includes(k) ? '$' : ''}${money(run.summary[k], ['pnl', 'fees'].includes(k) ? 2 : 0)}`}</b></div>)}</div>
        <Panel title="Profit and loss over time"><EquityChart equity={run.equity || []} /></Panel>
        <Panel title="Orders placed"><ObjectTable rows={run.orders || []} columns={['client_order_id', 'ticker', 'side', 'price', 'initial_count', 'fill_count', 'remaining_count', 'status', 'created_at']} /></Panel>
        <Panel title="Completed trades"><ObjectTable rows={run.fills || []} columns={['trade_id', 'ticker', 'side', 'price', 'count', 'fee', 'is_taker', 'ts_ms']} /></Panel>
        <details className="advanced-panel"><summary>More detail: contracts and payouts</summary>
            <Panel title="Contracts held"><ObjectTable rows={run.positions || []} columns={['ticker', 'position', 'cost', 'realized_pnl', 'fees_paid']} /></Panel>
            <Panel title="Market payouts"><ObjectTable rows={run.settlements || []} columns={['ticker', 'result', 'position', 'payout', 'pnl']} /></Panel>
            <pre>{JSON.stringify(run.summary, null, 2)}</pre>
        </details>
    </>;
}
function ObjectTable({ rows, columns }: {
    rows: any[];
    columns: string[];
}) { return <Table columns={columns.map(friendlyLabel)} rows={rows.map(r => columns.map(c => {
    const value = r[c];
    if (value == null) return 'Not available';
    if (c === 'ts_ms') return new Date(Number(value)).toLocaleString();
    if (c === 'created_at') return localDate(String(value));
    if (typeof value === 'boolean') return value ? 'Yes' : 'No';
    if (c === 'status') return friendlyLabel(String(value));
    return String(value);
}))} empty="Nothing to show yet." />; }
function EquityChart({ equity }: {
    equity: [
        number,
        number
    ][];
}) { const points = equity.map(([t, v]) => ({ ts_ms: t, pnl: String(v) })); return <div className="simple-equity">{points.map((p, i) => <span key={i} title={`${utc(p.ts_ms)} - $${money(p.pnl)}`} style={{ height: `${Math.max(4, 50 + Number(p.pnl) * 2)}%` }}/>)}</div>; }
function Bots({ user, notify }: {
    user: User;
    notify: Notice;
}) { const [bots, setBots] = useState<Bot[]>([]), [jobs, setJobs] = useState<Job[]>([]), [strategies, setStrategies] = useState<any[]>([]), [windows, setWindows] = useState<WindowRecord[]>([]), [keys, setKeys] = useState<any[]>([]), [status, setStatus] = useState<any>(null), [loading, setLoading] = useState(true), [show, setShow] = useState(false), [editing, setEditing] = useState<string | null>(null), [name, setName] = useState(''), [strategy, setStrategy] = useState(''), [series, setSeries] = useState('KXBTC15M'), [keyId, setKeyId] = useState(''), [params, setParams] = useState<Record<string, any>>({}), [selected, setSelected] = useState<string[]>([]), [sim, setSim] = useState({ latency_ms: 50, queue_model: 'front', fee_multiplier: '1.00', starting_balance: '1000.00' }), [liveBot, setLiveBot] = useState<Bot | null>(null), [typed, setTyped] = useState(''); const refresh = useCallback(async () => { try {
    const [b, j, s, w, k, st] = await Promise.all([api<Bot[]>('/api/bots'), api<Job[]>('/api/jobs?limit=50'), api<any[]>('/api/strategies'), api<WindowRecord[]>('/api/windows?limit=100'), api<any[]>('/api/keys'), api<any>('/api/status')]);
    setBots(b);
    setJobs(j);
    setStrategies(s);
    setWindows(w);
    setKeys(k);
    setStatus(st);
    if (!strategy && s.length)
        setStrategy(s[0].name);
}
catch (e) {
    notify((e as Error).message);
}
finally {
    setLoading(false);
} }, [notify]); useEffect(() => { refresh(); const timer = setInterval(refresh, 3000); return () => clearInterval(timer); }, [refresh]); const chosen = strategies.find(s => s.name === strategy); useEffect(() => { if (chosen && !editing) {
    const values: Record<string, any> = {};
    chosen.params.forEach((p: any) => values[p.name] = p.default);
    setParams(values);
} }, [strategy, chosen?.name, editing]); const saveBot = async (e: React.FormEvent) => { e.preventDefault(); try {
    await api(editing ? `/api/bots/${editing}` : '/api/bots', { method: editing ? 'PATCH' : 'POST', body: JSON.stringify({ name, strategy, params, series: [series], key_id: keyId || null }) });
    notify(editing ? 'Bot updated' : 'Bot created');
    setShow(false);
    setEditing(null);
    setName('');
    refresh();
}
catch (e) {
    notify((e as Error).message);
} }; const start = async (bot: Bot, kind: 'sim' | 'paper' | 'live') => { if (kind === 'sim' && !selected.length) {
    notify('Choose at least one recording below to test your bot.');
    return;
} if (kind === 'live') {
    setTyped('');
    setLiveBot(bot);
    return;
} try {
    await api('/api/jobs', { method: 'POST', body: JSON.stringify({ kind, bot_id: bot.id, ...(kind === 'sim' ? { windows: selected, sim } : {}) }) });
    notify(`${friendlyLabel(kind)} is waiting to start`);
    refresh();
}
catch (e) {
    notify((e as Error).message);
} }; const stop = async (id: string) => { try {
    await api(`/api/jobs/${id}/stop`, { method: 'POST' });
    notify('Stop requested');
    refresh();
}
catch (e) {
    notify((e as Error).message);
} }; const liveAllowed = !!user.live_enabled && !!status?.live_trading_enabled && !status?.kill_switch?.global && !status?.kill_switch?.user; const confirmLive = async () => { if (!liveBot || typed !== liveBot.name)
    return; try {
    await api('/api/jobs', { method: 'POST', body: JSON.stringify({ kind: 'live', bot_id: liveBot.id }) });
    setLiveBot(null);
    setTyped('');
    notify('Live job queued');
    refresh();
}
catch (e) {
    notify((e as Error).message);
} }; return <><PageTitle title="My bots" subtitle="Choose how your bot trades, test it, and stay in control." action={<Button kind="primary" onClick={() => { setEditing(null); setName(''); setShow(true); }}>New bot</Button>}/>
    <div className="mode-guide"><div><Icon name="replay" /><b>1. Test past markets</b><p>Try your bot on saved recordings. No real money.</p></div><div><Icon name="shield" /><b>2. Practice live</b><p>Use today's prices with pretend money.</p></div><div><Icon name="chart" /><b>3. Trade real money</b><p>Send real orders to Kalshi. Losses are possible.</p></div></div>
    {(status?.kill_switch?.global || status?.kill_switch?.user) && <div className="error-box">The safety pause is on. New practice and real-money sessions cannot start.</div>}<Panel title="Safety pause for my account"><p className="panel-help">Stops your practice and real-money sessions and prevents new ones from starting. Past-market tests are still available.</p><div className="inline-actions" style={{ padding: '12px 22px 20px' }}><Tag tone={status?.kill_switch?.user ? 'bad' : 'good'}>{status?.kill_switch?.user ? 'Paused' : 'Ready'}</Tag><Button kind="danger" onClick={async () => { const reason = prompt('Why are you pausing trading?'); if (!reason)
    return; try {
    await api('/api/kill-switch', { method: 'POST', body: JSON.stringify({ scope: 'user', engaged: true, reason }) });
    notify('Your trading is paused');
    refresh();
}
catch (e) {
    notify((e as Error).message);
} }}>Pause my trading</Button><Button onClick={async () => { try {
    await api('/api/kill-switch', { method: 'POST', body: JSON.stringify({ scope: 'user', engaged: false, reason: 'Released by account owner' }) });
    notify('Safety pause removed. Stopped bots will not restart automatically.');
    refresh();
}
catch (e) {
    notify((e as Error).message);
} }}>Remove my pause</Button></div></Panel><Panel title="Your bots">{loading ? <Busy /> : bots.length ? <div className="bot-list">{bots.map(bot => <div className="bot-card" key={bot.id}><div className="bot-info"><b>{bot.name}</b><span>{friendlyLabel(bot.strategy)} · {bot.series.map(marketName).join(', ')}</span><small>Updated {localDate(bot.updated_at)}</small></div><div className="bot-actions"><Button onClick={() => start(bot, 'sim')}>Test past markets</Button><Button onClick={() => start(bot, 'paper')}>Practice live</Button><Button kind="danger-outline" disabled={!liveAllowed || !bot.key_id} title="Needs a Kalshi connection, administrator approval, and no safety pause" onClick={() => start(bot, 'live')}>Trade real money</Button><Button onClick={() => { setEditing(bot.id); setName(bot.name); setStrategy(bot.strategy); setSeries(bot.series[0] || 'KXBTC15M'); setKeyId(bot.key_id || ''); setParams(bot.params || {}); setShow(true); }}>Edit</Button><Button kind="quiet" onClick={async () => { if (confirm(`Delete bot ${bot.name}?`)) {
    try {
        await api(`/api/bots/${bot.id}`, { method: 'DELETE' });
        notify('Bot deleted');
        refresh();
    }
    catch (e) {
        notify((e as Error).message);
    }
} }}>Delete</Button></div></div>)}</div> : <Empty>Create your first bot, then choose a recording below to test it.</Empty>}</Panel>
    <Panel title="Recordings for your past-market test"><p className="panel-help">Select one or more recordings, then choose “Test past markets” on your bot.</p>
        <div className="window-select">{windows.map(w => <label key={w.window_id}><input type="checkbox" checked={selected.includes(w.window_id)} onChange={e => setSelected(e.target.checked ? [...selected, w.window_id] : selected.filter(id => id !== w.window_id))} /><span>{marketName(w.series_ticker)} · {localDate(w.open_time)}</span><small>{friendlyLabel(w.quality)}</small></label>)}</div>
        <div className="sim-config"><label>Pretend starting balance ($)<input value={sim.starting_balance} onChange={e => setSim({ ...sim, starting_balance: e.target.value })} /></label></div>
        <details className="advanced-panel"><summary>Optional: fine-tune your test</summary><div className="sim-config">
            <label>Order delay (milliseconds)<input type="number" min="0" value={sim.latency_ms} onChange={e => setSim({ ...sim, latency_ms: Number(e.target.value) })} /><small>1000 milliseconds = 1 second.</small></label>
            <label>Place in the waiting line<select value={sim.queue_model} onChange={e => setSim({ ...sim, queue_model: e.target.value })}><option value="front">First in line</option><option value="pro_rata">Share by order size</option><option value="back">Last in line</option></select><small>How waiting orders are matched in the test.</small></label>
            <label>Fee adjustment<input value={sim.fee_multiplier} onChange={e => setSim({ ...sim, fee_multiplier: e.target.value })} /><small>1 uses normal fees. 2 doubles them.</small></label>
        </div></details>
    </Panel>
    <Panel title="Bot sessions"><Table columns={['Session', 'Trading mode', 'Status', 'Bot', 'Started', 'Action']} rows={jobs.map(j => [j.run_id ? <Link className="text-link" to={`/runs/${j.run_id}`}>#{j.id}</Link> : <span>#{j.id}</span>, <Tag>{j.kind}</Tag>, <Tag tone={j.status === 'running' ? 'good' : j.status === 'failed' ? 'bad' : ''}>{j.status}</Tag>, bots.find(b => b.id === j.bot_id)?.name || j.bot_id, localDate(j.created_at), ['running', 'queued'].includes(j.status) ? <Button kind="danger-outline" onClick={() => stop(j.id)}>Stop</Button> : 'Finished'])} /></Panel>
    {jobs.map(j => <JobEvents key={j.id} job={j} />)}
    {show && <Modal title={editing ? 'Edit bot' : 'Create bot'} close={() => { setShow(false); setEditing(null); }}><form className="form-grid" onSubmit={saveBot}>
        <label>Bot name<input required value={name} onChange={e => setName(e.target.value)} /></label>
        <label>Trading approach<select value={strategy} onChange={e => setStrategy(e.target.value)}>{strategies.map(s => <option key={s.name} value={s.name}>{friendlyLabel(s.name)}</option>)}</select><small>Choose the rules your bot follows.</small></label>
        <label>Coin<select value={series} onChange={e => setSeries(e.target.value)}><option value="KXBTC15M">Bitcoin</option><option value="KXETH15M">Ethereum</option></select></label>
        <label>Kalshi connection<select value={keyId} onChange={e => setKeyId(e.target.value)}><option value="">No connection · practice only</option>{keys.map(k => <option key={k.id} value={k.id}>{k.label} ({friendlyLabel(k.environment)})</option>)}</select></label>
        {chosen?.params.map((p: any) => <label key={p.name}>{friendlyLabel(p.name)}{p.name === 'ref_source' ? <select value={params[p.name] ?? p.default} onChange={e => setParams({ ...params, [p.name]: e.target.value })}><option value="coinbase">Coinbase</option><option value="cf">Market reference (CF Benchmarks)</option></select> : p.type === 'bool' ? <input type="checkbox" checked={!!params[p.name]} onChange={e => setParams({ ...params, [p.name]: e.target.checked })} /> : <input type={['int', 'float', 'decimal'].includes(p.type) ? 'number' : 'text'} step={p.type === 'int' ? 1 : 'any'} min={p.min} max={p.max} value={params[p.name] ?? ''} onChange={e => setParams({ ...params, [p.name]: p.type === 'str' ? e.target.value : p.type === 'int' ? Number(e.target.value) : e.target.value })} />}<small>{parameterHelp(p.name, p.help)}</small></label>)}
        <div className="form-actions"><Button onClick={() => setShow(false)}>Cancel</Button><Button type="submit" kind="primary">Save bot</Button></div>
    </form></Modal>}
    {liveBot && <Modal title="Start real-money trading?" close={() => setLiveBot(null)}><div className="warning-box">This bot will place real orders in your Kalshi account. You can lose money. This is not a practice session.</div><label>Type “{liveBot.name}” to confirm<input value={typed} onChange={e => setTyped(e.target.value)} /></label><div className="form-actions"><Button onClick={() => setLiveBot(null)}>Cancel</Button><Button kind="danger" disabled={typed !== liveBot.name || !liveAllowed} onClick={confirmLive}>Start real-money trading</Button></div></Modal>}
    </>;
}
function JobEvents({ job }: {
    job: Job;
}) { const [events, setEvents] = useState<any[]>([]); useEffect(() => { api<any[]>(`/api/jobs/${job.id}/events?limit=50`).then(setEvents).catch(() => { }); }, [job.id, job.status]); if (!events.length)
    return null; return <details className="advanced-panel"><summary>Session #{job.id}: updates and details</summary><Table columns={['Time', 'Update', 'What happened']} rows={events.map(e => [localDate(e.ts), <Tag tone={e.level === 'error' ? 'bad' : e.level === 'warn' ? 'degraded' : 'good'}>{e.level}</Tag>, e.message])} /></details>; }
function Keys({ notify }: {
    notify: Notice;
}) { const [items, setItems] = useState<any[]>([]), [show, setShow] = useState(false), [label, setLabel] = useState(''), [env, setEnv] = useState('production'), [keyId, setKeyId] = useState(''), [pem, setPem] = useState(''); const load = () => api<any[]>('/api/keys').then(setItems).catch(e => notify((e as Error).message)); useEffect(() => { load(); }, []); const add = async (e: React.FormEvent) => { e.preventDefault(); try {
    await api('/api/keys', { method: 'POST', body: JSON.stringify({ label, environment: env, api_key_id: keyId, private_key_pem: pem }) });
    setPem('');
    setShow(false);
    notify('Connection saved. Your private key stays hidden.');
    load();
}
catch (e) {
    notify((e as Error).message);
} }; return <><PageTitle title="Kalshi connection" subtitle="Connect your account so your bot can trade on Kalshi." action={<Button kind="primary" onClick={() => setShow(true)}>Add connection</Button>} /><div className="intro-note"><Icon name="shield" /> Your private key is encrypted and stays hidden after you save it. Connecting an account does not start trading.</div><Panel title="Your connections"><Table columns={['Name', 'Account type', 'Key reference', 'Status', 'Last used', 'Using this connection', 'Actions']} rows={items.map(k => [k.label, <Tag>{k.environment}</Tag>, <code>{k.api_key_id}</code>, <Tag tone={k.is_active ? 'good' : 'bad'}>{k.is_active ? 'active' : 'disabled'}</Tag>, localDate(k.last_used_at), k.leased_by_job_id ? `Session #${k.leased_by_job_id}` : 'Not in use', <div className="inline-actions"><Button onClick={async () => { try {
        const r = await api<any>(`/api/keys/${k.id}/test`, { method: 'POST' });
        notify(r.ok ? `Key test passed${r.balance ? ` - balance $${money(r.balance)}` : ''}` : `Key test failed: ${r.error}`);
    }
    catch (e) {
        notify((e as Error).message);
    } }}>Test</Button><Button kind="danger-outline" onClick={async () => { if (confirm(`Delete ${k.label}?`)) {
        try {
            await api(`/api/keys/${k.id}`, { method: 'DELETE' });
            notify('Key deleted');
            load();
        }
        catch (e) {
            notify((e as Error).message);
        }
    } }}>Delete</Button></div>])} empty="No account connected yet. You can still watch replays and practice." /></Panel>{show && <Modal title="Connect your Kalshi account" close={() => setShow(false)}><p className="intro-note">In your Kalshi account settings, create an API key. Copy the key ID and the private key below. These let the bot access your account; never share them publicly.</p><form className="form-grid" onSubmit={add}><label>Connection name<input required value={label} onChange={e => setLabel(e.target.value)} placeholder="For example: My Kalshi account" /></label><label>Account type<select value={env} onChange={e => setEnv(e.target.value)}><option value="production">Real Kalshi account</option><option value="demo">Kalshi practice account</option></select></label><label className="wide">Key ID from Kalshi<input required value={keyId} onChange={e => setKeyId(e.target.value)} /><small>Copy the ID shown next to your API key.</small></label><label className="wide">Private key from Kalshi<textarea required rows={8} value={pem} onChange={e => setPem(e.target.value)} placeholder="-----BEGIN PRIVATE KEY-----" /><small>Paste the entire key, including the BEGIN and END lines.</small></label><div className="warning-box wide">Keep a secure backup. You will not be able to view this private key here after saving it.</div><div className="form-actions"><Button onClick={() => setShow(false)}>Cancel</Button><Button type="submit" kind="primary">Save connection</Button></div></form></Modal>}</>; }
function RiskEditor({ value, save }: {
    value: any;
    save: (v: any) => void;
}) { const [form, setForm] = useState<any>(value || {}); useEffect(() => setForm(value || {}), [value]); if (!value)
    return <Empty>Safety limits are not available right now.</Empty>; return <div className="risk-editor">{Object.keys(value).map(k => <label key={k}>{friendlyLabel(k)}<input type="number" step={k.includes('count') || k.includes('orders') ? 1 : 'any'} value={form[k] ?? ''} onChange={e => setForm({ ...form, [k]: e.target.value })}/></label>)}<Button kind="primary" onClick={() => save(Object.fromEntries(Object.entries(form).map(([k, v]) => [k, k.includes('count') || k.includes('orders') ? Number(v) : String(v)])))}>Save limits</Button></div>; }
function ServiceSummary({ title, description, value, icon }: { title: string; description: string; value: unknown; icon: IconName }) {
    let message = 'No update available', tone = '';
    if (Array.isArray(value)) message = `${value.length} bot workers reporting`;
    else if (value && typeof value === 'object') {
        const report = value as Record<string, unknown>;
        if (report.ok === false || report.ready === false || report.error) { message = 'Needs attention'; tone = 'bad'; }
        else if (report.ok === true || report.ready === true) { message = 'Ready'; tone = 'good'; }
        else message = 'Update received';
    }
    return <div className="status-card"><Icon name={icon} /><small>{title}</small><b className={tone}>{message}</b><p>{description}</p></div>;
}
async function patchUser(user: any, patch: any, notify: Notice, refresh: () => void) { try {
    await api(`/api/users/${user.id}`, { method: 'PATCH', body: JSON.stringify(patch) });
    notify('User updated');
    refresh();
}
catch (e) {
    notify((e as Error).message);
} }
function Admin({ notify }: {
    notify: Notice;
}) { const [status, setStatus] = useState<any>(null), [users, setUsers] = useState<any[]>([]), [fidelity, setFidelity] = useState<any[]>([]), [limits, setLimits] = useState<any>(null), [riskTarget, setRiskTarget] = useState(''), [riskDraft, setRiskDraft] = useState<any>(null), [show, setShow] = useState(false), [username, setUsername] = useState(''), [password, setPassword] = useState(''), [role, setRole] = useState('user'), [reason, setReason] = useState(''); const load = async () => { try {
    const [s, u, f, l] = await Promise.all([api<any>('/api/status'), api<any[]>('/api/users'), api<any[]>('/api/fidelity?limit=50'), api<any>('/api/risk-limits')]);
    setStatus(s);
    setUsers(u);
    setFidelity(f);
    setLimits(l);
}
catch (e) {
    notify((e as Error).message);
} }; useEffect(() => { load(); }, []); const updateLimits = async (scope: string, value: any) => { try {
    const result = await api<any>(scope === 'global' ? '/api/risk-limits/global' : `/api/risk-limits/${scope}`, { method: 'PUT', body: JSON.stringify(value) });
    if (scope === 'global')
        setLimits({ ...limits, global: result });
    setRiskDraft(result);
    notify('Risk limits saved');
}
catch (e) {
    notify((e as Error).message);
} }; const kill = async (scope: 'global' | 'user', engaged: boolean) => { if (engaged && !reason.trim()) {
    notify('Enter a reason before pausing trading.');
    return;
} try {
    await api('/api/kill-switch', { method: 'POST', body: JSON.stringify({ scope, engaged, reason }) });
    setReason('');
    notify(engaged ? 'Trading paused for everyone.' : 'Safety pause removed. Stopped bots will not restart automatically.');
    load();
}
catch (e) {
    notify((e as Error).message);
} }; return <><PageTitle title="Settings" subtitle="Manage accounts, check the system, and set safety limits." /><Panel title="How the system is doing"><div className="status-grid">
    <ServiceSummary title="Market recorder" description="Collects prices and trades." value={status?.collector} icon="replay" />
    <ServiceSummary title="Recording storage" description="Keeps market recordings safe." value={status?.ingest} icon="shield" />
    <ServiceSummary title="Replay library" description="Makes past markets available to watch." value={status?.data_api} icon="chart" />
    <ServiceSummary title="Bot workers" description="Run your tests and trading sessions." value={status?.workers} icon="bot" />
    </div><details className="advanced-panel"><summary>Technical details for troubleshooting</summary><pre>{JSON.stringify(status, null, 2)}</pre></details></Panel>
    <Panel title="Real-money trading and safety"><p className="panel-help">Real-money trading also needs approval for each account. Pausing everyone stops practice and real-money sessions. Turning permission back on does not restart stopped bots.</p><div className="kv"><span>Real-money trading permission</span><Tag>{status == null ? 'Not available' : status.live_trading_enabled ? 'On' : 'Off'}</Tag><span>Safety pause for everyone</span><Tag tone={status?.kill_switch?.global ? 'bad' : ''}>{status == null ? 'Not available' : status.kill_switch?.global ? 'Paused' : 'Not paused'}</Tag></div><label className="reason-label">Reason for pausing<input value={reason} onChange={e => setReason(e.target.value)} placeholder="For example: checking an unexpected trade" /></label><div className="inline-actions" style={{ padding: '0 22px 22px' }}><Button kind="danger" disabled={!status} onClick={() => kill('global', true)}>Pause everyone's trading</Button><Button disabled={!status} onClick={() => kill('global', false)}>Remove everyone's pause</Button><Button disabled={!status} onClick={async () => { const enabled = !status?.live_trading_enabled; try {
    await api('/api/settings/live-trading', { method: 'PUT', body: JSON.stringify({ enabled }) });
    notify(`Real-money trading permission ${enabled ? 'on' : 'off'}`);
    load();
}
catch (e) {
    notify((e as Error).message);
} }}>{status?.live_trading_enabled ? 'Turn off' : 'Allow'} real-money trading</Button></div></Panel><Panel title="Accounts" action={<Button kind="primary" onClick={() => setShow(true)}><Icon name="plus" /> Add account</Button>}><Table columns={['Username', 'Access level', 'Can sign in', 'Can trade real money', 'Created', 'Actions']} rows={users.map(u => [u.username, <select aria-label={`Access level for ${u.username}`} value={u.role} onChange={e => patchUser(u, { role: e.target.value }, notify, load)}><option value="user">Member</option><option value="admin">Administrator</option></select>, <input aria-label={`Allow ${u.username} to sign in`} type="checkbox" checked={u.is_active} onChange={e => patchUser(u, { is_active: e.target.checked }, notify, load)} />, <input aria-label={`Allow ${u.username} to trade real money`} type="checkbox" checked={u.live_enabled} onChange={e => patchUser(u, { live_enabled: e.target.checked }, notify, load)} />, localDate(u.created_at), <Button onClick={async () => { const next = prompt(`New password for ${u.username}`); if (next)
        patchUser(u, { password: next }, notify, load); }}>Reset password</Button>])} /></Panel><Panel title="Trading safety limits"><p className="panel-help">Set how much each bot may trade and lose. These limits reduce risk, but do not guarantee a profit or prevent every loss.</p><h3>Default limits for everyone</h3><RiskEditor value={limits?.global} save={v => updateLimits('global', v)} /><h3>Custom limits for one account</h3><div className="inline-actions" style={{ padding: '8px 22px' }}><select aria-label="Account for custom safety limits" value={riskTarget} onChange={e => { setRiskTarget(e.target.value); setRiskDraft(limits?.global || null); }}><option value="">Choose an account</option>{users.map(u => <option key={u.id} value={u.id}>{u.username}</option>)}</select></div>{riskTarget && <RiskEditor value={riskDraft} save={v => updateLimits(riskTarget, v)} />}<p className="muted">Saving custom limits applies them to the selected account.</p></Panel><details className="advanced-panel"><summary>Replay accuracy checks</summary><p className="panel-help">Checks compare our recordings and simulated trades with market data. Passing a check is not a promise of profit.</p><Table columns={['Recording', 'Check', 'Result', 'Details', 'Created']} rows={fidelity.map(f => [f.window_id || 'Multiple recordings', { G1: 'Price matching', G2: 'Trade matching', G3: 'Trading rules', G4: 'Real trade comparison' }[f.gate as string] || f.gate, <Tag tone={f.passed ? 'good' : 'bad'}>{f.passed ? 'Passed' : 'Needs attention'}</Tag>, <details><summary>View details</summary><pre>{JSON.stringify(f.metrics, null, 2)}</pre></details>, localDate(f.created_at)])} /></details>{show && <Modal title="Add an account" close={() => setShow(false)}><form className="form-grid" onSubmit={async (e) => { e.preventDefault(); try {
    await api('/api/users', { method: 'POST', body: JSON.stringify({ username, password, role }) });
    notify('User created');
    setShow(false);
    load();
}
catch (e) {
    notify((e as Error).message);
} }}><label>Username<input required value={username} onChange={e => setUsername(e.target.value)} /></label><label>Temporary password<input required type="password" value={password} onChange={e => setPassword(e.target.value)} /></label><label>Access level<select value={role} onChange={e => setRole(e.target.value)}><option value="user">Member</option><option value="admin">Administrator</option></select></label><div className="form-actions"><Button onClick={() => setShow(false)}>Cancel</Button><Button type="submit" kind="primary">Create account</Button></div></form></Modal>}</>; }
function App() { const [user, setUser] = useState<User | null>(null), [ready, setReady] = useState(false), [menu, setMenu] = useState(false), [toast, setToast] = useState(''); const location = useLocation(), navigate = useNavigate(); const notify = useCallback((text: string) => { setToast(text); window.setTimeout(() => setToast(''), 3500); }, []); useEffect(() => { api<{
    user: User;
    csrf: string;
}>('/api/auth/me').then(r => { setUser(r.user); sessionStorage.setItem('csrf', r.csrf); }).catch(() => { }).finally(() => setReady(true)); const expired = () => { setUser(null); navigate('/login'); }; const failed = (e: Event) => notify((e as CustomEvent<string>).detail); window.addEventListener('auth:expired', expired); window.addEventListener('api:error', failed); return () => { window.removeEventListener('auth:expired', expired); window.removeEventListener('api:error', failed); }; }, []); const login = (u: User, csrf: string) => { sessionStorage.setItem('csrf', csrf); setUser(u); navigate('/live'); }; const logout = async () => { try {
    await api('/api/auth/logout', { method: 'POST' });
}
catch { } sessionStorage.removeItem('csrf'); setUser(null); navigate('/login'); }; if (!ready)
    return <div className="loading full"><i />Connecting...</div>; if (!user)
    return location.pathname === '/login' ? <Login onLogin={login}/> : <Navigate to="/login" replace/>; if (location.pathname === '/login')
    return <Navigate to="/live" replace/>;
    const nav: [string, string, IconName][] = [['/live', 'Live markets', 'chart'], ['/windows', 'Market replays', 'replay'], ['/research', 'Agent research', 'sparkles'], ['/runs', 'Results', 'chart'], ['/bots', 'My bots', 'bot'], ['/keys', 'Kalshi connection', 'key'], ['/admin', 'Settings', 'settings']];
    return <div className="app-shell">
        <a className="skip-link" href="#main-content">Skip to content</a>
        <aside className="sidebar"><Link className="logo" to="/windows" aria-label={APP_NAME}><span className="brand-mark small"><Icon name="sparkles" /></span><span className="brand-name">AQLab<small>Kalshi Scalper Center</small></span></Link><div className="nav-label">YOUR SPACE</div><nav aria-label="Main navigation">{nav.filter(([path]) => path !== '/admin' || user.role === 'admin').map(([path, label, icon]) => <Link key={path} aria-label={label} title={label} aria-current={location.pathname.startsWith(path) ? 'page' : undefined} className={location.pathname.startsWith(path) ? 'active' : ''} to={path}><Icon name={icon} className="nav-icon" /><span>{label}</span></Link>)}</nav><div className="sidebar-foot"><Icon name="shield" /><b>Practice first.</b><small>Real-money trades can lose money. You decide when to start.</small></div></aside>
        <main className="main-area" id="main-content"><header className="topbar"><div className="topbar-brand"><span>AQLab</span><strong>Kalshi Scalper Center</strong></div><div className="user-area">{import.meta.env.VITE_MOCK === '1' && <span className="demo-badge">Demo preview</span>}<span className="user-pill"><i>{user.username.slice(0, 1).toUpperCase()}</i>{user.username}</span><button className="menu-btn" aria-label="Account menu" aria-expanded={menu} onClick={() => setMenu(!menu)}><Icon name="user" /></button>{menu && <div className="user-menu"><button onClick={async () => { const current = prompt('Current password'), next = current && prompt('New password'); if (!current || !next)
    return; try {
    await api('/api/auth/password', { method: 'POST', body: JSON.stringify({ current_password: current, new_password: next }) });
    notify('Password updated');
}
catch (e) {
    notify((e as Error).message);
} }}><Icon name="key" /> Change password</button><button onClick={logout}><Icon name="logout" /> Sign out</button></div>}</div></header><div className="page"><Routes><Route path="/" element={<Navigate to="/live"/>}/><Route path="/live" element={<LiveMarkets />}/><Route path="/windows" element={<Windows />}/><Route path="/research" element={<Research />}/><Route path="/replay/:windowId" element={<Replay />}/><Route path="/runs" element={<Runs />}/><Route path="/runs/:id" element={<RunDetail />}/><Route path="/bots" element={<Bots user={user} notify={notify}/>}/><Route path="/keys" element={<Keys notify={notify}/>}/><Route path="/admin" element={user.role === 'admin' ? <Admin notify={notify}/> : <Navigate to="/windows"/>}/><Route path="*" element={<Navigate to="/live"/>}/></Routes></div></main>{toast && <div className="toast" role="status">{toast}</div>}</div>; }
export default App;
