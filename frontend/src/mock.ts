import type {Frame,Overview,WindowRecord,LiveSnapshot} from './api';
const base=Date.now()-15*60_000;
export function mockLiveStream(receive: (snapshot: LiveSnapshot) => void) {
 const emit = () => {
  const now = Date.now(), open = Math.floor(now / 900000) * 900000;
  receive({ ts_ms: now, kalshi_connected: true, coinbase_connected: true,
   markets: ['KXBTC15M', 'KXETH15M'].map((series, i) => ({
    series_ticker: series, market_ticker: `${series}-${open}`, open_ms: open, close_ms: open + 900000,
    strike: i ? '3200' : '62500', status: 'active', yes_bid: '0.45', yes_ask: '0.47',
    last: (.46 + Math.sin(now / 10000) * .03).toFixed(4), price_ms: now, quote_ms: now,
    ref: ((i ? 3200 : 62500) + Math.sin(now / 7000) * 20 + 1).toFixed(2), ref_ms: now,
    coinbase: ((i ? 3200 : 62500) + Math.sin(now / 7000) * 20).toFixed(2), coinbase_ms: now,
   })) });
 };
 emit();
 const timer = setInterval(emit, 100);
 return () => clearInterval(timer);
}
export const demoWindow:WindowRecord={window_id:'demo-btc-001',market_ticker:'KXBTC15M-26OCT101845-45',series_ticker:'KXBTC15M',open_time:new Date(base).toISOString(),close_time:new Date(base+900000).toISOString(),result:null,strike:'62500.00',event_count:5240,gap_count:0,quality:'ok',size_bytes:172400};
const series=Array.from({length:901},(_,i)=>{const p=.48+Math.sin(i/77)*.08+Math.sin(i/17)*.015;return{ts_ms:base+i*1000,yes_bid:(p-.01).toFixed(4),yes_ask:(p+.01).toFixed(4),mid:p.toFixed(4),last:p.toFixed(4),volume:(i%10===0?'3.00':'0.00'),ref:(62500+Math.sin(i/53)*130).toFixed(2),coinbase:(62500+Math.sin(i/51)*134).toFixed(2)}});
export const demoOverview:Overview={window:demoWindow,start_ms:base,end_ms:base+900000,open_ms:base,close_ms:base+900000,series,trades:series.filter((_,i)=>i%13===0).map(p=>({ts_ms:p.ts_ms,price:p.mid||'0.5',count:'3.00',taker_side:'yes'})),depth:series.filter((_,i)=>i%5===0).map(p=>({ts_ms:p.ts_ms,bids:[[p.yes_bid||'0.4','120.00'],['0.4000','78.00']],asks:[[p.yes_ask||'0.5','80.00'],['0.6000','42.00']]})),lifecycle:[{ts_ms:base,event_type:'opened',text:'Market opened'}]};
const researchTokens: { id: string; label: string; expires_at: string }[] = [];
const researchProposal = {
 id: 1, experiment_id: 1, candidate: 0, rationale: 'Demo only: inspect the approval workflow. These are not real research results.',
 status: 'pending', job_id: null as number | null,
 spec: { bot: { name: 'Demo baseline', strategy: 'momentum', params: { take_profit: '0.04', stop_loss: '0.05' }, series: ['KXBTC15M'] },
  sim: { latency_ms: 100, queue_model: 'back', fee_multiplier: 1 }, duration_seconds: 3600,
  risk_limits: { max_order_count: '1', max_position_per_market: '2', max_daily_loss: '3' } },
};
async function mockResearch(path: string, init: RequestInit): Promise<unknown> {
 const clean = path.split('?')[0], method = (init.method || 'GET').toUpperCase();
 if (clean === '/api/research/tokens') {
  if (method === 'GET') return [...researchTokens];
  if (method === 'POST') {
   const body = JSON.parse(String(init.body));
   const token = { id: `demo-token-${Date.now()}`, label: body.label, expires_at: new Date(Date.now() + 30 * 86400000).toISOString() };
   researchTokens.push(token);
   return { ...token, token: 'DEMO-NOT-A-REAL-RESEARCH-TOKEN' };
  }
 }
 if (clean.startsWith('/api/research/tokens/') && method === 'DELETE') {
  const index = researchTokens.findIndex(token => token.id === clean.split('/').pop());
  if (index < 0) throw new Error('Demo research token not found');
  researchTokens.splice(index, 1); return undefined;
 }
 if (clean === '/api/research/experiments' && method === 'GET') {
  return [{ id: 1, name: 'Demo experiment', hypothesis: 'Preview the research UI; no strategy edge is asserted.', created_at: new Date(base).toISOString() }];
 }
 if (clean === '/api/research/experiments/1' && method === 'GET') {
  return { id: 1, name: 'Demo experiment', success_criteria: 'Real evidence must come from recorded-market simulations.',
   plan: { candidates: [researchProposal.spec.bot], train_windows: ['demo-training'], test_windows: ['demo-test'] },
   notes: [{ text: 'Preview data only, not measured profits.', at: new Date(base).toISOString() }] };
 }
 if (clean === '/api/research/experiments/1/results' && method === 'GET') {
  return { trials: [{ candidate: 0, label: 'Demo baseline', split: 'train', valid_for_comparison: false,
   job: { id: 1, status: 'succeeded', error: null, run_id: null }, summary: { pnl: '0', fees: '0', n_fills: 0 } }] };
 }
 if (clean === '/api/research/paper-proposals' && method === 'GET') return [researchProposal];
 if (clean.startsWith('/api/research/paper-proposals/1/') && method === 'POST') {
  if (researchProposal.status !== 'pending') throw new Error('Demo proposal already decided');
  const decision = clean.split('/').pop();
  if (decision !== 'approve' && decision !== 'reject') throw new Error('Invalid demo decision');
  researchProposal.status = decision === 'approve' ? 'approved' : 'rejected';
  researchProposal.job_id = decision === 'approve' ? 2 : null;
  return researchProposal;
 }
 if (clean === '/api/research/jobs/2/stop' && method === 'POST') return { id: 2, status: 'canceled' };
 throw new Error('This research operation is unavailable in demo preview');
}
export async function mockApi<T>(path:string,init:RequestInit={}):Promise<T>{const cleanPath=path.split("?")[0];const method=(init.method||'GET').toUpperCase(); if(path==='/api/auth/me')return {user:{id:'demo',username:'demo',role:'admin',is_active:true,live_enabled:false,created_at:new Date().toISOString()},csrf:'demo-csrf'} as T; if(path==='/api/auth/login')return {user:{id:'demo',username:'demo',role:'admin',is_active:true,live_enabled:false,created_at:new Date().toISOString()},csrf:'demo-csrf'} as T;if(path==='/api/auth/logout'||path==='/api/auth/password')return undefined as T;
 if(path.startsWith('/api/research/'))return await mockResearch(path,init) as T;
 if(path==='/api/live-markets'){const now=Date.now(),open=Math.floor(now/900000)*900000;return{ts_ms:now,kalshi_connected:true,coinbase_connected:true,markets:['KXBTC15M','KXETH15M'].map((name,i)=>({series_ticker:name,market_ticker:`${name}-${open}`,open_ms:open,close_ms:open+900000,strike:i?'3200':'62500',status:'active',yes_bid:'0.45',yes_ask:'0.47',last:(.46+Math.sin(now/10000)*.03).toFixed(4),price_ms:now,quote_ms:now,ref:i?'3201':'62501',ref_ms:now,coinbase:((i?3200:62500)+Math.sin(now/7000)*20).toFixed(2),coinbase_ms:now}))} as T;}
 if(path.startsWith('/api/windows/')&&path.endsWith('/overview'))return demoOverview as T;if(path.startsWith('/api/windows/'))return demoWindow as T;if(path.startsWith('/api/windows')){
  const query=new URL(path,'http://demo.local').searchParams;
  return Array.from({length:14},(_,i)=>({...demoWindow,window_id:`demo-${i}`,series_ticker:i%2?'KXETH15M':'KXBTC15M',open_time:new Date(base-i*900000).toISOString(),result:i%2?'no':'yes',quality:i%3?'degraded':'ok'}))
   .filter(w=>(!query.get('series')||w.series_ticker===query.get('series'))&&(!query.get('from')||w.open_time>=query.get('from')!)&&(!query.get('to')||w.open_time<=query.get('to')!)).slice(0,Number(query.get('limit')||100)) as T;
 }
 if(path==='/api/strategies')return[{name:'momentum',description:'Momentum demo strategy',params:[{name:'lookback_seconds',type:'int',default:30,min:1,max:300,help:'Lookback window'},{name:'threshold',type:'decimal',default:'0.03',min:'0',max:'1',help:'Entry threshold'}]}] as T;
 if(path.startsWith('/api/bots'))return (method==='POST'?{id:'demo-bot',name:'Demo bot',strategy:'momentum',params:{},series:['KXBTC15M'],active_job:null}:[]) as T;if(path.startsWith('/api/jobs'))return (path.includes('/events')?[]:[]) as T;if(path.startsWith('/api/runs'))return (path.split('/').length>3?{id:'demo-run',job_id:'demo-job',bot_id:'demo-bot',mode:'sim',strategy:'momentum',params:{},sim_config:{},windows:[demoWindow.window_id],status:'succeeded',started_at:new Date(base).toISOString(),finished_at:new Date(base+900000).toISOString(),summary:{pnl:'12.40',fees:'0.42',n_orders:12,n_fills:8,volume:'140.00'},orders:[],fills:[],positions:[],settlements:[],equity:[[base,0],[base+300000,4],[base+600000,2],[base+900000,12.4]]}:[]) as T;
 if(cleanPath==='/api/keys'||cleanPath==='/api/users'||cleanPath==='/api/fidelity')return [] as T;if(path==='/api/status')return{collector:{status:'running',subscribed_markets:2,spool_backlog:0,uploader_errors:0},ingest:{status:'ok'},data_api:{ok:true},workers:[],live_trading_enabled:false,kill_switch:{global:false,user:false}} as T;if(path==='/api/risk-limits')return{global:{max_order_count:20,max_position_per_market:100,max_open_orders:10,max_daily_loss:'100.00',max_notional_per_market:'500.00',max_orders_per_minute:30},user:null} as T;return {ok:true} as T;}
export function mockReplay(onFrame:(f:Frame)=>void){let ts=demoOverview.start_ms;let playing=false,speed=1;const emit=()=>{const p=demoOverview.series[Math.min(demoOverview.series.length-1,Math.floor((ts-demoOverview.start_ms)/1000))];onFrame({type:'frame',ts_ms:ts,playing,speed,book:{ticker:demoWindow.market_ticker,valid:true,yes_bids:[[p?.yes_bid||'0.4','120.00'],['0.3900','80.00']],yes_asks:[[p?.yes_ask||'0.5','75.00'],['0.5100','45.00']],mid:p?.mid||'0.45',spread:'0.02'},trades:demoOverview.trades.filter(t=>t.ts_ms<=ts).slice(-8),ticker:{price:p?.last||'0.45',volume:'1200.00',open_interest:'800.00'},ref:p?.ref||null,coinbase:p?.coinbase||null,events:[]})};emit();const timer=window.setInterval(()=>{if(playing){ts=Math.min(demoOverview.end_ms,ts+100*speed);emit();if(ts>=demoOverview.end_ms)playing=false}},100);return{send(raw:string){const m=JSON.parse(raw);if(m.op==='play')playing=true;if(m.op==='pause')playing=false;if(m.op==='speed')speed=m.speed;if(m.op==='seek')ts=Math.min(demoOverview.end_ms,Math.max(demoOverview.start_ms,m.ts_ms));if(m.op==='step'){playing=false;ts=Math.min(demoOverview.end_ms,Math.max(demoOverview.start_ms,ts+m.ms))}emit()},close(){clearInterval(timer)}}}
