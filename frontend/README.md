# AQLab Kalshi Scalper Center

React + TypeScript + Vite interface for the Kalshi momentum bot. The interface uses an iOS-inspired
dark glass style, inline SVG icons, and plain-language labels.

## Interface

- **Market replays**: choose a recording, then play, pause, rewind, or change playback speed.
- **Results**: see profit/loss, orders placed, and completed trades.
- **My bots**: test past markets, practice live with pretend money, or explicitly confirm real-money trading.
- **Kalshi connection**: connect an account; private keys remain hidden after saving.
- **Settings** (administrators): manage accounts, safety pauses, and trading limits.

Exchange codes, API field names, and trading-mode values remain unchanged. Technical diagnostics and
advanced test settings are in expandable sections. Practice and real-money trading remain clearly
distinguished; existing permissions and safety checks still apply.

The layout includes compact icon navigation on tablets and bottom navigation on phones. Controls keep
text labels or accessible names; reduced-motion preferences are respected.

## Development

From `frontend/`, install dependencies with `npm ci`, then run `npm run dev`. The Vite dev server proxies `/api` and WebSocket `/ws` requests to `http://localhost:8800`. To demo without a backend, run `VITE_MOCK=1 npm run dev` (PowerShell: `$env:VITE_MOCK="1"; npm run dev`). Mock credentials accept any username and password. A visible “Demo preview” badge distinguishes sample data from a live account.

## Checks and production build

- `npm run lint` — TypeScript check
- `npm test` — unit tests
- `npm run build` — type-check and create `frontend/dist`

The production bundle uses base path `/`. The backend serves `frontend/dist` at `/` with SPA fallback; API calls use relative same-origin paths and the session cookie. The replay client sends `X-KMB-CSRF` on state-changing requests using the token returned by login or `/api/auth/me`.
