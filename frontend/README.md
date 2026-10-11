# AQLab Kalshi Scalper Center

React + TypeScript + Vite interface for the Kalshi momentum bot. The interface uses an iOS-inspired
dark glass style, inline SVG icons, and plain-language labels.

## Interface

- **Live markets** (home): Bitcoin and Ethereum stacked in full-width cards, refreshed every second, with
  current 15-minute windows, prices to beat, YES/NO costs and one graph per coin.
  Each card separates the local-time market window and large `mm:ss` countdown
  from three labeled tiles: Kalshi price, price to beat, and dollars above/below
  target (or at target). The timer runs independently of network requests.
  Stale prices remain labeled as not live and never produce a current target comparison.
  Green shows Kalshi's coin price, blue shows Coinbase's price, and a dashed gold
  line marks the price to beat. There is no separate YES-price graph.
  Charts collect up to 15 minutes while the page is open and reset when each market rolls over.
  Disconnections and missing prices are shown explicitly; viewing does not place orders.
- **Market replays**: choose a recording, then play, pause, rewind, or change playback speed.
  Starts with the latest six recordings; **Load more** adds six at a time. Refresh
  and filter changes reset the list to six. At the API's 1,000-record limit, use
  date filters to find older markets. YES outcomes are green and NO outcomes red.
  Recording explanations follow the overall quality check, not just the gap count:
  recovered interruptions can still mean missing events even if the gap counter is zero.
- **Results**: see profit/loss, orders placed, and completed trades.
- **My bots**: test past markets, practice live with pretend money, or explicitly confirm real-money trading.
- **Kalshi connection**: connect an account; private keys remain hidden after saving.
- **Settings** (administrators): manage accounts, safety pauses, and trading limits.

Exchange codes, API field names, and trading-mode values remain unchanged. Technical diagnostics and
advanced test settings are in expandable sections. Practice and real-money trading remain clearly
distinguished; existing permissions and safety checks still apply.

The layout includes compact icon navigation on tablets and bottom navigation on phones. Controls keep
text labels or accessible names; reduced-motion preferences are respected.
Shared surfaces across live markets, replays, results, bots, settings, and sign-in use
translucent dark glass, background blur, subtle highlights, and consistent borders.
Browsers without backdrop blur use a readable dark fallback.

## Development

From `frontend/`, install dependencies with `npm ci`, then run `npm run dev`. The Vite dev server proxies `/api` and WebSocket `/ws` requests to `http://localhost:8800`. To demo without a backend, run `VITE_MOCK=1 npm run dev` (PowerShell: `$env:VITE_MOCK="1"; npm run dev`). Mock credentials accept any username and password. A visible “Demo preview” badge distinguishes sample data from a live account.

## Checks and production build

- `npm run lint` — TypeScript check
- `npm test` — unit tests
- `npm run build` — type-check and create `frontend/dist`

The production bundle uses base path `/`. The backend serves `frontend/dist` at `/` with SPA fallback; API calls use relative same-origin paths and the session cookie. The replay client sends `X-KMB-CSRF` on state-changing requests using the token returned by login or `/api/auth/me`.
