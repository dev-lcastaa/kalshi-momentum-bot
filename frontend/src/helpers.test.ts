import {describe,expect,it} from 'vitest';
import {bucketDepth,formatCents,formatDecimal,frameReducer} from './helpers';
describe('decimal formatting',()=>{
 it('rounds decimal strings exactly',()=>{expect(formatDecimal('0.4200')).toBe('0.42');expect(formatDecimal('999.999')).toBe('1000.00');expect(formatDecimal('-1.235')).toBe('-1.24');expect(formatDecimal(null)).toBe('N/A')});
 it('converts contract prices to cents without floating point math',()=>{expect(formatCents('0.4200')).toBe('42.00 cents');expect(formatCents('0.99995')).toBe('100.00 cents');expect(formatCents(null)).toBe('N/A')});
});
describe('replay frame reducer',()=>{it('replaces playhead state with latest server frame',()=>{const old={ts_ms:1,playing:true},next={ts_ms:2,playing:false};expect(frameReducer(old,next)).toBe(next)})});
describe('depth bucketing',()=>{it('aggregates levels into ordered time buckets',()=>{expect(bucketDepth([{bids:[['.4','2']],asks:[['.6','3']]},{bids:[['.3','4']],asks:[]}],1)).toEqual([{bids:6,asks:3}])})});
