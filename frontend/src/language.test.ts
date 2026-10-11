import { describe, expect, it } from 'vitest';
import { APP_NAME, friendlyLabel, marketName, outcomeTone, recordingHelp } from './language';

describe('plain-language interface', () => {
  it('uses the requested product name', () => {
    expect(APP_NAME).toBe('AQLab Kalshi Scalper Center');
  });
  it('keeps practice and real money unmistakably different', () => {
    expect(friendlyLabel('paper')).toContain('no real money');
    expect(friendlyLabel('live')).toContain('Real-money');
    expect(friendlyLabel('calibration')).toBe('Replay accuracy check');
  });
  it('explains statuses and money limits without changing API values', () => {
    expect(friendlyLabel('queued')).toBe('Waiting to start');
    expect(friendlyLabel('MAX DAILY LOSS')).toBe('Daily loss limit ($)');
    expect(friendlyLabel('realized_pnl')).toContain('closed trades');
    expect(friendlyLabel('a_new_field')).toBe('a new field');
  });
  it('shows coin names instead of exchange codes', () => {
    expect(marketName('KXBTC15M')).toBe('Bitcoin');
    expect(marketName('KXETH15M')).toBe('Ethereum');
  });
  it('colors YES and NO outcomes independently of whether settlement exists', () => {
    expect(outcomeTone('yes')).toBe('good');
    expect(outcomeTone('no')).toBe('bad');
    expect(outcomeTone(null)).toBe('');
  });
  it('does not confuse recovered interruptions with a clean recording', () => {
    expect(friendlyLabel('degraded')).toBe('Recovered interruptions');
    expect(recordingHelp('degraded')).toContain('some events may be missing');
    expect(recordingHelp('degraded')).not.toContain('No gaps');
    expect(recordingHelp('bad')).toContain('unrecovered');
    expect(recordingHelp('ok')).toContain('No gaps');
    expect(recordingHelp('unknown')).toContain('not been checked');
  });
});
