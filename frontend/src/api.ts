export type User={id:string;username:string;role:'admin'|'user';is_active:boolean;live_enabled:boolean;created_at:string};
export type WindowRecord={window_id:string;market_ticker:string;series_ticker:string;open_time:string;close_time:string;result:'yes'|'no'|null;strike:string;event_count:number;gap_count:number;quality:'ok'|'degraded'|'bad'|'unknown';size_bytes:number};
export type Point={ts_ms:number;yes_bid:string|null;yes_ask:string|null;mid:string|null;last:string|null;volume:string|null;ref:string|null;coinbase:string|null};
export type Trade={ts_ms:number;price:string;count:string;taker_side:'yes'|'no'};
export type Depth={ts_ms:number;bids:[string,string][];asks:[string,string][]};
export type ReplayEvent={ts_ms:number;kind:'lifecycle'|'transport'|'gap';text:string};
export type Overview={window:WindowRecord;start_ms:number;end_ms:number;open_ms:number;close_ms:number;series:Point[];trades:Trade[];depth:Depth[];lifecycle:{ts_ms:number;event_type:string;text:string}[]};
export type Frame={type:'frame';ts_ms:number;playing:boolean;speed:number;book:{ticker:string;valid:boolean;yes_bids:[string,string][];yes_asks:[string,string][];mid:string|null;spread:string|null}|null;trades:Trade[];ticker:{price:string;volume:string;open_interest:string}|null;ref:string|null;coinbase:string|null;events:ReplayEvent[]};
export type Job={id:string;kind:string;status:string;bot_id:string;spec:Record<string,any>;worker_id:string|null;cancel_requested:boolean;error:string|null;created_at:string;started_at:string|null;finished_at:string|null;heartbeat_at:string|null;run_id:string|null};
export type Bot={id:string;name:string;strategy:string;params:Record<string,any>;series:string[];key_id:string|null;created_at:string;updated_at:string;active_job:Job|null};
const mock=import.meta.env.VITE_MOCK==='1';
export async function api<T>(path:string,init:RequestInit={}):Promise<T>{
 if(mock){const {mockApi}=await import('./mock');return mockApi<T>(path,init)}
 const headers=new Headers(init.headers); if(init.body&&!headers.has('Content-Type'))headers.set('Content-Type','application/json');
 const csrf=sessionStorage.getItem('csrf'); if(csrf&&!['GET','HEAD'].includes((init.method||'GET').toUpperCase())&&!path.endsWith('/auth/login'))headers.set('X-KMB-CSRF',csrf);
 let res:Response;try{res=await fetch(path,{...init,headers,credentials:'same-origin'})}catch(e){window.dispatchEvent(new CustomEvent('api:error',{detail:(e as Error).message||'Network request failed'}));throw e}
 if(res.status===401&&path!=='/api/auth/me'&&path!=='/api/auth/login'){window.dispatchEvent(new Event('auth:expired'));throw new Error('Session expired')}
 if(!res.ok){let msg=`Request failed (${res.status})`;try{msg=(await res.json()).detail||msg}catch{}window.dispatchEvent(new CustomEvent('api:error',{detail:msg}));throw new Error(msg)}
 if(res.status===204)return undefined as T;return await res.json() as T;
}
export function wsUrl(path:string){return `${location.protocol==='https:'?'wss':'ws'}://${location.host}${path}`}
