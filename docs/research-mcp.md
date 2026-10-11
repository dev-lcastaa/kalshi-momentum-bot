# Agent scalping research

The local stdio MCP server (`python -m kmb.mcp`) is a research workbench over
`/api/research` on the existing backend. Existing simulation/trade workers execute
jobs; the MCP process does not run a second exchange or access the database.

## Permissions

- Research tokens can read recorded markets, create immutable experiments,
  submit simulation batches, read research results, append findings, propose
  paper sessions, and stop research jobs.
- They cannot authenticate ordinary backend endpoints, read Kalshi keys, modify
  trading bots, manage users/tokens, approve paper proposals, or start live jobs.
- Tokens belong to one user, expire after at most 90 days, are stored as hashes,
  and can be revoked in the **Agent research** page. Admin-owned research tokens
  still see only their owner's research, not other users' research.
- Paper approval requires the owner's normal browser session plus CSRF. Approval
  starts the exact immutable proposal once. There is no MCP approval tool.

Treat the browser account as human-only. Do not give agents its password, session
cookie, CSRF token, admin access, or a generic authenticated browser alongside
the research token; doing so would defeat the separation.

## Setup

1. Deploy the updated backend/UI and workers using the existing Jenkins workflow.
   Startup adds four research tables through the existing `init_db`/`create_all`
   mechanism; existing tables and columns are unchanged. Back up the database
   before deployment as usual.
2. Leave real-money trading disabled. Keep a simulation worker running. Paper
   sessions also need a paper-capable trade worker and a healthy NATS feed.
   The recording-only deployment has zero trade workers; enabling paper workers
   does not itself enable real-money trading.
   The **Agent research** page puts pending practice decisions and study results
   first. Choose **View results** to see plain-language outcomes, net profit/loss,
   drawdown and trade counts. Settings and older practice decisions are expandable;
   saved findings can be paged without displaying raw JSON.
3. Sign in as the research owner, open **Agent research**, expand **Connect or
   manage a research agent**, and choose **Create agent access** to create a token.
   Save the one-time value in a private file **outside the repository and
   OneDrive**, for example `C:\Users\luis0\.kmb\research-token`. Restrict the file
   to your account. Dismiss the value in the UI after saving it.
4. On the agent workstation, install the project's locked dependencies with
   `uv sync`. The MCP client launches `python -m kmb.mcp` through that environment.
5. Use HTTPS for remote backend access, or an SSH tunnel to localhost. Plain HTTP
   to a remote host is intentionally rejected to avoid transmitting the research
   token unencrypted. A Windows OpenSSH example for node003 is:

   ```powershell
   ssh -N -L 127.0.0.1:8800:127.0.0.1:8800 lcastaa@192.168.1.181
   ```

   Keep the tunnel running while the MCP client is connected. If local port 8800
   is occupied, choose a different local port and update `KMB_MCP_BACKEND_URL`.

Example VS Code MCP configuration (merge into your user or workspace MCP
configuration; the token itself must not appear in this file):

```json
{
  "servers": {
    "kalshi-research": {
      "type": "stdio",
      "command": "uv",
      "args": [
        "--directory",
        "C:\\Users\\luis0\\OneDrive\\Documents\\Python-Projects\\kalshi-momentum-bot",
        "run",
        "python",
        "-m",
        "kmb.mcp"
      ],
      "env": {
        "KMB_MCP_BACKEND_URL": "http://127.0.0.1:8800",
        "KMB_MCP_TOKEN_FILE": "C:\\Users\\luis0\\.kmb\\research-token"
      }
    }
  }
}
```

This server opens no listening port and needs no additional container.
Use `research_capabilities` to verify the connection and discover strategy schemas.
If a token expires or is revoked, replace its file and restart the MCP connection.
Errors such as missing recordings, denied permissions and exceeded budgets are
returned as tool errors, not empty successful results.

**Privacy:** a local MCP server does not make a hosted agent private. Tool
responses can be transmitted to the agent's model provider. Connect only a
model/provider approved for this market data, or use a local model if it must
remain on your infrastructure. Raw Parquet files are not exposed; summaries,
recording overviews, settings and simulated order/fill records are.

## Research workflow

1. Call `research_capabilities` and `list_recordings`. Inspect quality, gaps,
   settlement and dates; use paginated `recording_overview` for prices, spreads,
   liquidity and reference-price behavior. Overview data is sampled in one-second
   buckets, not a substitute for tick-level execution data.
2. Write a hypothesis and success criteria **before** running simulations.
   `create_experiment` accepts explicit candidates and disjoint chronological
   training/test window IDs. Expand a small parameter grid into candidates.
   Include nearby settings and realistic latency stress cases.
3. Call `submit_simulation_batch` for `train`. It returns job IDs immediately;
   use job status/results rather than holding a tool call open.
4. Once training succeeds, submit `test` for the same immutable candidates.
   Evaluate `compare_experiment`, then inspect paginated per-window and fill
   details with `research_run`.
5. Append evidence, negative results, limitations and next hypotheses using
   `record_finding`. Future agents can use `list_experiments`/`get_experiment`.
   Notes are newest-first and paginated (default 5, maximum 20); use `notes_offset`
   and `notes_limit` to retrieve older findings.
6. For a successfully tested candidate, use `propose_paper_session`. The human
   reviews its rationale, settings, duration and risk limits in **Agent research**
   and approves or rejects it. Approval starts the paper job; agents can inspect
   the resulting job ID via `list_paper_proposals`, monitor it, and stop it.

An example experiment body:

```json
{
  "name": "Momentum baseline versus latency stress",
  "hypothesis": "The baseline remains net profitable after fees with slower execution.",
  "success_criteria": "Positive held-out net P&L, acceptable drawdown, enough fills, and no dependence on one window.",
  "train_windows": ["<earlier completed window ID>"],
  "test_windows": ["<later completed window ID>"],
  "candidates": [
    {
      "label": "baseline",
      "strategy": "momentum",
      "params": {"take_profit": "0.04", "stop_loss": "0.05"},
      "series": ["KXBTC15M"],
      "sim": {"latency_ms": 100, "queue_model": "back", "fee_multiplier": 1}
    },
    {
      "label": "latency-stress",
      "strategy": "momentum",
      "params": {"take_profit": "0.04", "stop_loss": "0.05"},
      "series": ["KXBTC15M"],
      "sim": {"latency_ms": 500, "queue_model": "back", "fee_multiplier": 1}
    }
  ]
}
```

Do not select repeatedly against the test period and still call it untouched.
The server enforces within-experiment split separation, not cross-experiment
statistical independence. Reserve fresh later data for final validation.
Compare candidates on like-for-like coins/windows; different candidate series
do not represent the same dataset. No trades, too few trades, or no convincing
edge are valid outcomes. Neither agent reasoning nor profitable backtests
guarantee small profits.

## Bounds and reproducibility

- At most 12 candidates and 100 windows per split per experiment; duplicate IDs,
  unsupported series, incomplete/gapped/unsettled recordings, missing content
  hashes, overlapping or reversed splits are rejected.
- Parameters resolve to explicit defaults. Sim defaults are 100 ms latency,
  back queue, full fees and $1,000 starting balance. Fee multipliers below 1
  and non-finite/invalid research settings are rejected.
- At most 24 queued/running simulation jobs per owner, including ordinary sim
  jobs. Submissions serialize on the owner row in Postgres. A repeated submission
  of an existing split returns its existing jobs, including failed/canceled ones.
  Create a new experiment for retries; there is no hidden partial batch retry.
- Experiment plans persist window metadata/content hashes, resolved parameters,
  simulator options, a fixed seed of 0, and a source-content engine fingerprint.
  Workers reject mismatched code or changed/missing window metadata. The manifest
  stores the archive's declared content hash; this is not a new byte-level archive
  integrity audit. `get_experiment(include_manifest=true)` exposes full manifests.
- Each window resets strategy/exchange state, as in the existing simulator.
  Aggregate P&L is a sum across independent windows, not a compounded portfolio.
  P&L already includes fees: do not subtract them again.
- Comparison results flag failed/incomplete runs and summarize profitable,
  losing, flat and traded windows, plus worst/best/mean per-window net P&L.
  These are window-level statistics, not round-trip win rates.
  Detailed windows/orders/fills
  are paginated (maximum 200 items); job summaries omit large specs/equity curves.
- At most 10 pending paper proposals and one active paper job per owner.
  Proposed duration is 1-1,440 minutes. The worker enforces it with a monotonic
  clock from job-control initialization, including idle/disconnected feeds,
  then uses normal shutdown/cancellation. Starting positions cannot be promised
  a guaranteed exit fill; inspect remaining positions in results.
- Paper risk caps snapshot the account's existing risk limits when proposed.
  Workers apply the stricter of these approved caps and current account limits
  on each control refresh. Existing risk-guard semantics still apply; these are
  not guarantees against losses. Kill switches remain effective.
- Proposed settings cannot be patched. Any change requires a new proposal and
  approval. There are no arbitrary strategy-upload, shell, credential, approval
  or live-trading tools. New strategy implementations require ordinary code
  changes, tests and review.

## MCP tools

`research_capabilities`, `list_recordings`, `recording_overview`,
`create_experiment`, `list_experiments`, `get_experiment`, `record_finding`,
`submit_simulation_batch`, `compare_experiment`, `research_job_status`,
`research_run`, `stop_research_job`, `propose_paper_session`,
`list_paper_proposals`.
