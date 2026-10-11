export function formatDecimal(value:string|null|undefined,places=2):string{
 if(value==null||value==='')return 'N/A';
 const precision=Math.max(0,Math.floor(places));
 const negative=value.trim().startsWith('-');
 const raw=negative?value.trim().slice(1):value.trim();
 const [whole='0',fraction='']=raw.split('.');
 let scaled=BigInt((whole||'0')+(fraction+'0'.repeat(precision)).slice(0,precision));
 if(fraction[precision]&&fraction[precision]>='5')scaled+=1n;
 const unit=10n**BigInt(precision);let integer=(scaled/unit).toString();
 const decimal=precision?(scaled%unit).toString().padStart(precision,'0'):'';
 if(precision&&BigInt(decimal)>=unit){integer=(BigInt(integer)+1n).toString()}
 return `${negative?'-':''}${integer}${precision?'.'+decimal:''}`;
}
export function formatCents(value:string|null|undefined):string{
 if(value==null||value==='')return 'N/A';
 const raw=value.trim(),negative=raw.startsWith('-'),unsigned=negative?raw.slice(1):raw;
 const [whole='0',fraction='']=unsigned.split('.');
 const divisor=10n**BigInt(fraction.length);
 const numerator=BigInt((whole||'0')+fraction)*10000n;
 const rounded=(numerator+divisor/2n)/divisor;
 return `${negative?'-':''}${rounded/100n}.${(rounded%100n).toString().padStart(2,'0')} cents`;
}
export function frameReducer(_state:any,frame:any){return frame}
export function bucketDepth(depth:any[],count:number){if(!depth.length||count<1)return[];const out=Array.from({length:count},()=>({bids:0,asks:0}));depth.forEach((d,i)=>{const n=Math.min(count-1,Math.floor(i*count/depth.length));out[n].bids+=d.bids.reduce((a:any,x:any)=>a+Number(x[1]),0);out[n].asks+=d.asks.reduce((a:any,x:any)=>a+Number(x[1]),0)});return out}
