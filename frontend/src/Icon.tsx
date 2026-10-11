export type IconName = 'replay' | 'chart' | 'bot' | 'key' | 'settings' | 'play' | 'pause' | 'back' | 'forward' | 'refresh' | 'plus' | 'shield' | 'arrow' | 'close' | 'user' | 'logout' | 'sparkles' | 'clock' | 'target' | 'up' | 'down';

const paths: Record<IconName, string> = {
  clock: 'M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0M12 7v5l3 2',
  target: 'M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0M16 12a4 4 0 1 1-8 0 4 4 0 0 1 8 0M12 10v4M10 12h4',
  up: 'M12 20V4M5 11l7-7 7 7',
  down: 'M12 4v16M5 13l7 7 7-7',
  replay: 'M3 11a9 9 0 1 1 2.7 7M3 4v7h7M10 8l6 4-6 4z',
  chart: 'M4 3v17h17M7 14l4-4 4 3 5-7',
  bot: 'M12 3v3M10 3h4M6 7h12a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2zM8 11v2M16 11v2M9 17h6M1 11v5M23 11v5',
  key: 'M15 3a6 6 0 1 1-4.7 9.7L3 20H1v-4l7.3-7.3A6 6 0 0 1 15 3zM16 7h.01',
  settings: 'M9 3h6l1 3 3 1 2 5-2 5-3 1-1 3H9l-1-3-3-1-2-5 2-5 3-1zM15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0',
  play: 'M8 5l11 7-11 7z',
  pause: 'M8 5v14M16 5v14',
  back: 'M11 6l-6 6 6 6M5 12h15',
  forward: 'M13 6l6 6-6 6M19 12H4',
  refresh: 'M20 7a8 8 0 0 0-14-2L3 8M3 3v5h5M4 17a8 8 0 0 0 14 2l3-3M21 21v-5h-5',
  plus: 'M12 5v14M5 12h14',
  shield: 'M12 3l8 3v6c0 5-8 9-8 9s-8-4-8-9V6zM8 12l3 3 5-6',
  arrow: 'M5 12h14M13 6l6 6-6 6',
  close: 'M6 6l12 12M18 6L6 18',
  user: 'M16 7a4 4 0 1 1-8 0 4 4 0 0 1 8 0M4 21v-2a8 8 0 0 1 16 0v2',
  logout: 'M9 4H4v16h5M9 12h12M16 7l5 5-5 5',
  sparkles: 'M12 3l2.5 6.5L21 12l-6.5 2.5L12 21l-2.5-6.5L3 12l6.5-2.5z',
};

export function Icon({ name, className = '' }: { name: IconName; className?: string }) {
  return <svg className={`icon ${className}`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false"><path d={paths[name]} /></svg>;
}
