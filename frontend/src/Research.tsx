import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from './api';
import { Icon } from './Icon';
import { friendlyLabel, marketName } from './language';
import { researchMoney, researchResult, researchSetting } from './researchPresentation';

type Experiment = { id: number; name: string; hypothesis: string; created_at: string };
type Token = { id: string; label: string; expires_at: string };
type Candidate = {
    label?: string; name?: string; strategy: string;
    params: Record<string, unknown>; series: string[]; sim?: Record<string, unknown>;
};
type Proposal = {
    id: number; experiment_id: number; candidate: number; rationale: string;
    status: string; job_id: number | null;
    spec: { bot: Candidate; sim: Record<string, unknown>; duration_seconds: number; risk_limits: Record<string, string> };
};
type Trial = {
    candidate: number; label: string; split: string; valid_for_comparison: boolean;
    job: { id: number; status: string; error: string | null; run_id: number | null };
    summary: Record<string, unknown>; window_stats?: Record<string, unknown> | null;
    comparison_issue?: string | null;
};
type ExperimentDetail = {
    id: number; name: string; hypothesis: string; success_criteria: string;
    plan: { candidates: Candidate[]; train_windows: string[]; test_windows: string[] };
    notes: { text: string; at: string }[]; notes_total?: number; notes_offset?: number;
};
const notePageSize = 5;
const settingLabels: Record<string, string> = {
    latency_ms: 'Delay before an order reaches the market', queue_model: 'Place in the order queue',
    fee_multiplier: 'Trading fee assumption', starting_balance: 'Practice balance',
};

function Settings({ values }: { values: Record<string, unknown> }) {
    return <dl className="research-settings">{Object.entries(values).filter(([key]) => key !== 'seed').map(([key, value]) =>
        <div key={key}><dt>{settingLabels[key] || friendlyLabel(key)}</dt><dd>{researchSetting(key, value)}</dd></div>)}</dl>;
}

function TrialCard({ trial, busy, stop }: { trial: Trial; busy: boolean; stop: () => void }) {
    const active = ['queued', 'running'].includes(trial.job.status);
    return <article className="research-card">
        <div className="research-card-heading"><div><span className="eyebrow">{trial.split === 'train' ? 'Earlier markets' : 'Later markets · validation'}</span>
            <h4>{trial.label}</h4></div><span className={`tag ${trial.job.status === 'failed' ? 'bad' : ''}`}>{friendlyLabel(trial.job.status)}</span></div>
        <p className="research-verdict">{researchResult(trial.job.status, trial.valid_for_comparison, trial.summary.n_fills, trial.summary.pnl)}</p>
        <dl className="research-metrics">
            <div><dt>Profit / loss after fees</dt><dd>{researchMoney(trial.summary.pnl)}</dd></div>
            <div><dt>Largest drop in value</dt><dd>{researchMoney(trial.summary.max_drawdown)}</dd></div>
            <div><dt>Completed trades</dt><dd>{typeof trial.summary.n_fills === 'number' ? trial.summary.n_fills : 'Not available'}</dd></div>
        </dl>
        {!trial.valid_for_comparison && <p className="research-note">These results are not ready to compare with other tests.</p>}
        {trial.job.error && <div className="error-box" role="alert"><strong>The test could not finish.</strong><details><summary>Show the problem</summary><p>{trial.job.error}</p></details></div>}
        <details className="research-details"><summary>More about this test</summary>
            <Settings values={{ fees: trial.summary.fees, n_windows: trial.summary.n_windows,
                ...(trial.window_stats || {}) }} />
            {trial.comparison_issue && <p className="research-note">{trial.comparison_issue}</p>}
        </details>
        <div className="inline-actions">
            {trial.job.run_id && <Link className="button" to={`/runs/${trial.job.run_id}`}>View trade history</Link>}
            {active && <button className="button" disabled={busy} onClick={stop}>Stop test</button>}
        </div>
    </article>;
}

export default function Research() {
    const [experiments, setExperiments] = useState<Experiment[]>([]);
    const [proposals, setProposals] = useState<Proposal[]>([]);
    const [tokens, setTokens] = useState<Token[]>([]);
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(true);
    const [busy, setBusy] = useState(false);
    const [label, setLabel] = useState('Research agent');
    const [newToken, setNewToken] = useState('');
    const [selection, setSelection] = useState<{ detail: ExperimentDetail; trials: Trial[] } | null>(null);

    const load = useCallback(async () => {
        const [e, p, t] = await Promise.all([
            api<Experiment[]>('/api/research/experiments'),
            api<Proposal[]>('/api/research/paper-proposals'),
            api<Token[]>('/api/research/tokens'),
        ]);
        setExperiments(e); setProposals(p); setTokens(t);
    }, []);
    useEffect(() => {
        load().catch(e => setError((e as Error).message)).finally(() => setLoading(false));
    }, [load]);

    async function action(work: () => Promise<void>) {
        setBusy(true); setError('');
        try { await work(); await load(); }
        catch (e) { setError((e as Error).message); }
        finally { setBusy(false); }
    }

    async function inspect(experimentId: number, notesOffset = 0) {
        const [detail, result] = await Promise.all([
            api<ExperimentDetail>(`/api/research/experiments/${experimentId}?notes_offset=${notesOffset}&notes_limit=${notePageSize}`),
            api<{ trials: Trial[] }>(`/api/research/experiments/${experimentId}/results`),
        ]);
        setSelection({ detail, trials: result.trials });
    }
    const pending = proposals.filter(proposal => proposal.status === 'pending');
    const history = proposals.filter(proposal => proposal.status !== 'pending');

    function proposalCard(proposal: Proposal) {
        const decisionLabels: Record<string, string> = { pending: 'Your approval needed', approved: 'Approved', rejected: 'Declined' };
        return <article className="research-card" key={proposal.id}>
            <div className="research-card-heading"><div><span className="eyebrow">{proposal.spec.bot.series.map(marketName).join(' & ')} · practice only</span>
                <h3>{proposal.spec.bot.name || 'Proposed strategy'}</h3></div><span className="tag">{decisionLabels[proposal.status] || proposal.status}</span></div>
            <p>{proposal.rationale}</p>
            <dl className="research-metrics">
                <div><dt>Session length</dt><dd>{proposal.spec.duration_seconds / 60} minutes</dd></div>
                <div><dt>Loss limit</dt><dd>{researchMoney(proposal.spec.risk_limits.max_daily_loss)}</dd></div>
                <div><dt>Contracts per order, at most</dt><dd>{proposal.spec.risk_limits.max_order_count ?? 'Not available'}</dd></div>
            </dl>
            <details className="research-details"><summary>Review strategy settings and all safety limits</summary>
                <h4>{friendlyLabel(proposal.spec.bot.strategy)}</h4><Settings values={proposal.spec.bot.params} />
                <h4>How practice trades are simulated</h4><Settings values={proposal.spec.sim} />
                <h4>Safety limits</h4><Settings values={proposal.spec.risk_limits} />
                <p className="research-note">Your current account limits may make these limits stricter. Limits do not guarantee an exit price or prevent every loss.</p>
            </details>
            <div className="inline-actions">
                {proposal.status === 'pending' && <>
                    <button className="button primary" disabled={busy} onClick={() => {
                        if (!confirm(`Start "${proposal.spec.bot.name}" in practice mode for ${proposal.spec.duration_seconds / 60} minutes with the displayed settings? No real money will be traded.`)) return;
                        action(async () => { await api(`/api/research/paper-proposals/${proposal.id}/approve`, { method: 'POST' }); });
                    }}>Start practice session</button>
                    <button className="button" disabled={busy} onClick={() => action(async () => {
                        await api(`/api/research/paper-proposals/${proposal.id}/reject`, { method: 'POST' });
                    })}>Decline</button>
                </>}
                <button className="button" disabled={busy} onClick={() => action(() => inspect(proposal.experiment_id))}>Review test results</button>
                {proposal.job_id && <button className="button" disabled={busy} onClick={() => action(async () => {
                    await api(`/api/research/jobs/${proposal.job_id}/stop`, { method: 'POST' });
                })}>Stop practice session</button>}
            </div>
        </article>;
    }

    return <div className="research-page">
        <div className="page-title"><div><h1>Agent research</h1><p>Your agents test ideas. You review the results and decide what to practice.</p></div>
            <button className="button" disabled={busy || loading} onClick={() => action(async () => {
                if (selection) await inspect(selection.detail.id, selection.detail.notes_offset || 0);
            })}>Refresh</button></div>
        {error && <div className="error-box" role="alert">{error}</div>}
        <div className="research-safety"><Icon name="shield" /><span><strong>No real-money trading here.</strong> Agents can run tests, but only you can start a practice session.</span></div>
        {loading ? <div className="loading" role="status">Loading your research...</div> : <>
            <div className="summary-strip">
                <div><span className="summary-icon"><Icon name="replay" /></span><div><b>{experiments.length} {experiments.length === 1 ? 'study' : 'studies'} shown</b><small>Ideas tested on past markets</small></div></div>
                <div><span className="summary-icon green"><Icon name="shield" /></span><div><b>{pending.length} awaiting your decision</b><small>Practice proposals shown below</small></div></div>
                <div><span className="summary-icon purple"><Icon name="bot" /></span><div><b>Research only</b><small>No access to your trading keys or bots</small></div></div>
            </div>
            <section className="panel">
                <div className="panel-head"><h2>Ready for your review</h2></div>
                <p className="panel-help">Review the test results before starting. Practice uses simulated money and stops after the approved time.</p>
                {!pending.length && <div className="empty"><strong>Nothing to approve yet.</strong><p>Your agents will bring promising ideas here after testing them on later markets.</p></div>}
                <div className="research-card-list">{pending.map(proposalCard)}</div>
            </section>
            <section className="panel">
                <div className="panel-head"><h2>Ideas being tested</h2></div>
                <p className="panel-help">Each study keeps its original settings. A profit in one test does not mean a strategy will keep making money.</p>
                {!experiments.length && <div className="empty"><strong>Your research starts here.</strong><p>Connect an agent below, then ask it to test a scalping idea using your recordings.</p></div>}
                <div className="research-card-list">{experiments.map(experiment => <article className="research-study" key={experiment.id}>
                    <div><h3>{experiment.name}</h3><p>{experiment.hypothesis}</p><small>Created {new Date(experiment.created_at).toLocaleDateString()}</small></div>
                    <button className="button" disabled={busy} aria-pressed={selection?.detail.id === experiment.id}
                        onClick={() => action(() => inspect(experiment.id))}>View results</button>
                </article>)}</div>
                {selection && <div className="research-evidence">
                    <div className="research-card-heading"><h3>{selection.detail.name}</h3><button className="button" onClick={() => setSelection(null)}>Close results</button></div>
                    <p><strong>What would make this idea worthwhile?</strong><br />{selection.detail.success_criteria}</p>
                    <p className="research-note">Earlier markets help explore the idea. Later markets check the same settings on data that comes afterward.
                        Reusing those later results to tune settings makes them less reliable as an independent check.</p>
                    {!selection.trials.length && <div className="empty">No tests have started for this study yet.</div>}
                    <div className="research-trials">{selection.trials.map(trial =>
                        <TrialCard key={trial.job.id} trial={trial} busy={busy} stop={() => action(async () => {
                            await api(`/api/research/jobs/${trial.job.id}/stop`, { method: 'POST' });
                            await inspect(selection.detail.id, selection.detail.notes_offset || 0);
                        })} />)}</div>
                    <h4>What the agents have learned</h4>
                    {!selection.detail.notes.length && <p className="research-note">No findings saved yet.</p>}
                    {selection.detail.notes.map((note, index) => <blockquote className="research-finding" key={`${note.at}-${index}`}>
                        <p>{note.text}</p><small>{new Date(note.at).toLocaleString()}</small>
                    </blockquote>)}
                    <div className="inline-actions">
                        {(selection.detail.notes_offset || 0) > 0 && <button className="button" disabled={busy} onClick={() =>
                            action(() => inspect(selection.detail.id, Math.max(0, (selection.detail.notes_offset || 0) - notePageSize)))}>Newer findings</button>}
                        {(selection.detail.notes_offset || 0) + selection.detail.notes.length < (selection.detail.notes_total || 0) &&
                            <button className="button" disabled={busy} onClick={() => action(() =>
                                inspect(selection.detail.id, (selection.detail.notes_offset || 0) + notePageSize))}>Older findings</button>}
                    </div>
                    <details className="research-details"><summary>Markets and settings used in this study</summary>
                        <p>{selection.detail.plan.train_windows.length} earlier recordings · {selection.detail.plan.test_windows.length} later recordings</p>
                        {selection.detail.plan.candidates.map((candidate, index) => <div key={index}>
                            <h4>{candidate.label || candidate.name || `Settings ${index + 1}`} · {candidate.series.map(marketName).join(' & ')}</h4>
                            <Settings values={candidate.params} />{candidate.sim && <Settings values={candidate.sim} />}
                        </div>)}
                    </details>
                </div>}
            </section>
            {!!history.length && <details className="advanced-panel"><summary>Previous practice decisions ({history.length})</summary>
                <div className="research-card-list">{history.map(proposalCard)}</div></details>}
        </>}
        <details className="advanced-panel" open={newToken ? true : undefined}>
            <summary>Connect or manage a research agent</summary>
            <div className="research-access">
                <p>Create an access token for your agent. It can read recordings, run tests, and suggest practice sessions.
                    It cannot approve sessions, use your Kalshi keys, or change your trading bots.</p>
                <p className="research-note">Your agent's model provider may receive the data it reads. Only connect a model you trust with this information.</p>
                <form className="research-access-form" onSubmit={e => {
                    e.preventDefault();
                    action(async () => {
                        const result = await api<Token & { token: string }>('/api/research/tokens', {
                            method: 'POST', body: JSON.stringify({ label, days: 30 }),
                        });
                        setNewToken(result.token);
                    });
                }}>
                    <label>Agent name<input required maxLength={64} value={label} onChange={e => setLabel(e.target.value)} /></label>
                    <button className="button primary" disabled={busy || loading || !!newToken} type="submit">Create agent access</button>
                </form>
                {newToken && <div className="research-token">
                    <h3>Save your agent's access token</h3><p>This value is shown once. Save it in a private file outside this project and OneDrive. Access lasts 30 days.</p>
                    <input aria-label="New research token" readOnly value={newToken} />
                    <button className="button" onClick={() => setNewToken('')}>I've saved it</button>
                </div>}
                {tokens.map(token => <div className="research-study" key={token.id}>
                    <div><strong>{token.label}</strong><p>Access expires {new Date(token.expires_at).toLocaleString()}</p></div>
                    <button className="button danger" disabled={busy} onClick={() => {
                        if (!confirm(`Remove access for "${token.label}"? This agent will no longer be able to use this token.`)) return;
                        action(async () => { await api(`/api/research/tokens/${token.id}`, { method: 'DELETE' }); setNewToken(''); });
                    }}>Remove access</button>
                </div>)}
            </div>
        </details>
    </div>;
}
