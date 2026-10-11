"""Research-only API. Bearer credentials never authenticate ordinary backend routes."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from kmb.backend import serializers as S
from kmb.backend.app import (
    ACTIVE,
    ALLOWED_SERIES,
    DB,
    CurrentUser,
    SimOptions,
    _aware,
    _job_out,
    _source_call,
    ctx_of,
    current_user,
)
from kmb.backend.security import new_session_token, session_id_for
from kmb.db import ops
from kmb.db.models import (
    FillRow,
    Job,
    Order,
    ResearchExperiment,
    ResearchPaperProposal,
    ResearchToken,
    ResearchTrial,
    Run,
    User,
    utcnow,
)
from kmb.research import engine_fingerprint
from kmb.strategy.registry import STRATEGIES, describe, validate_params
from kmb.worker.guard import Limits

router = APIRouter(prefix="/api/research", tags=["research"])
MAX_CANDIDATES = 12
MAX_WINDOWS = 100
MAX_ACTIVE = 24
MAX_PAPER = 1


class StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class TokenBody(StrictBody):
    label: str = Field(min_length=1, max_length=64)
    days: int = Field(default=30, ge=1, le=90)


class Candidate(StrictBody):
    label: str = Field(min_length=1, max_length=64)
    strategy: str = "momentum"
    params: dict[str, Any] = Field(default_factory=dict)
    series: list[Literal["KXBTC15M", "KXETH15M"]] = Field(default_factory=lambda: ["KXBTC15M"])
    sim: SimOptions = Field(
        default_factory=lambda: SimOptions(latency_ms=100, queue_model="back", fee_multiplier=1)
    )


class ExperimentBody(StrictBody):
    name: str = Field(min_length=1, max_length=128)
    hypothesis: str = Field(min_length=1, max_length=4000)
    success_criteria: str = Field(min_length=1, max_length=4000)
    train_windows: list[str] = Field(min_length=1, max_length=MAX_WINDOWS)
    test_windows: list[str] = Field(min_length=1, max_length=MAX_WINDOWS)
    candidates: list[Candidate] = Field(min_length=1, max_length=MAX_CANDIDATES)


class NoteBody(StrictBody):
    text: str = Field(min_length=1, max_length=4000)


class BatchBody(StrictBody):
    split: Literal["train", "test"]


class PaperBody(StrictBody):
    candidate: int = Field(ge=0)
    rationale: str = Field(min_length=1, max_length=4000)
    duration_minutes: int = Field(default=60, ge=1, le=1440)


def research_user(request: Request, db: DB) -> User:
    authorization = request.headers.get("Authorization")
    if authorization is None:
        return current_user(request, db)
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "research bearer token required")
    row = db.get(ResearchToken, session_id_for(authorization[7:]))
    if row is None or (_aware(row.expires_at) or utcnow()) <= utcnow():
        raise HTTPException(401, "invalid or expired research token")
    user = db.get(User, row.user_id)
    if user is None or not user.is_active:
        raise HTTPException(401, "research account is inactive")
    return user


ResearchUser = Annotated[User, Depends(research_user)]


def _lock_user(db: Session, user: User) -> None:
    # Serialize submissions for one owner so parallel agents cannot exceed the queue budget.
    db.execute(select(User).where(User.id == user.id).with_for_update()).scalar_one()


def _experiment(db: Session, user: User, experiment_id: int) -> ResearchExperiment:
    row = db.get(ResearchExperiment, experiment_id)
    if row is None or row.user_id != user.id:
        raise HTTPException(404, "research experiment not found")
    return row


def _experiment_json(
    row: ResearchExperiment,
    include_manifest: bool = False,
    notes_offset: int = 0,
    notes_limit: int = 5,
) -> dict[str, Any]:
    plan = dict(row.plan)
    if not include_manifest:
        for split in ("train", "test"):
            plan[f"{split}_windows"] = [window["window_id"] for window in plan[f"{split}_windows"]]
    return {
        "id": row.id,
        "name": row.name,
        "hypothesis": row.hypothesis,
        "success_criteria": row.success_criteria,
        "plan": plan,
        "notes": list(reversed(row.notes))[notes_offset : notes_offset + notes_limit],
        "notes_total": len(row.notes),
        "notes_offset": notes_offset,
        "created_at": S.iso(row.created_at),
    }


def _candidate(candidate: Candidate) -> dict[str, Any]:
    try:
        cls = STRATEGIES.get(candidate.strategy)
        if cls is None:
            raise ValueError("unknown strategy")
        defaults = {p["name"]: p["default"] for p in cls.param_schema}
        params = validate_params(candidate.strategy, defaults | candidate.params)
        for value in params.values():
            if isinstance(value, float) and not Decimal(str(value)).is_finite():
                raise ValueError("parameters must be finite")
        if candidate.strategy == "momentum":
            if params["ref_source"] not in ("coinbase", "cf"):
                raise ValueError("ref_source must be coinbase or cf")
            for key in ("max_spread", "take_profit", "stop_loss"):
                if not 0 < Decimal(params[key]) <= 1:
                    raise ValueError(f"{key} must be in (0, 1]")
            if not 0 <= Decimal(params["min_entry_price"]) < Decimal(params["max_entry_price"]) <= 1:
                raise ValueError("entry price range must be within [0, 1]")
            for key in ("max_hold_s", "exit_before_close_s"):
                if params[key] <= 0:
                    raise ValueError(f"{key} must be positive")
            for key in ("cooldown_s", "no_entry_last_s"):
                if params[key] < 0:
                    raise ValueError(f"{key} must be non-negative")
    except (ValueError, ArithmeticError) as exc:
        raise HTTPException(422, str(exc)) from None
    if not candidate.series or len(set(candidate.series)) != len(candidate.series):
        raise HTTPException(422, "candidate series must be nonempty and unique")
    sim = candidate.sim.model_dump(exclude_none=True)
    sim = {"latency_ms": 100, "queue_model": "back", "fee_multiplier": 1, "starting_balance": 1000} | sim
    if sim["fee_multiplier"] < 1:
        raise HTTPException(422, "research cannot discount exchange fees")
    return {
        "label": candidate.label,
        "strategy": candidate.strategy,
        "params": params,
        "series": candidate.series,
        "sim": sim,
    }


def _windows(request: Request, ids: list[str]) -> list[dict[str, Any]]:
    if len(set(ids)) != len(ids):
        raise HTTPException(422, "duplicate windows are not allowed")
    raw = [_source_call(ctx_of(request).source.get_window, wid) for wid in ids]
    rows = [S.window_json(row) | {"sha256": row.get("sha256")} for row in raw]
    for row in rows:
        if row["quality"] != "ok" or row["gap_count"] != 0 or row["result"] not in ("yes", "no"):
            raise HTTPException(422, f"window {row['window_id']} must be complete, gap-free and settled")
        if row["series_ticker"] not in ALLOWED_SERIES:
            raise HTTPException(422, "unsupported market series")
        if not row["open_time"] or not row["close_time"]:
            raise HTTPException(422, "window timestamps are required")
        if not row["sha256"]:
            raise HTTPException(422, "recording content hash is required")
        try:
            if _date(row["close_time"]) <= _date(row["open_time"]):
                raise ValueError("window close must follow its open")
        except ValueError as exc:
            raise HTTPException(422, f"invalid window timestamps: {exc}") from None
    return sorted(rows, key=lambda row: row["open_time"])


def _date(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone-aware window timestamps required")
    return parsed.astimezone(UTC)


@router.post("/tokens", status_code=201)
def create_token(body: TokenBody, db: DB, user: CurrentUser) -> dict[str, Any]:
    token = new_session_token()
    row = ResearchToken(
        id=session_id_for(token),
        user_id=user.id,
        label=body.label,
        expires_at=utcnow() + timedelta(days=body.days),
    )
    db.add(row)
    db.flush()
    ops.log_event(db, "info", "research_token_created", body.label, user_id=user.id)
    return {"id": row.id, "token": token, "label": row.label, "expires_at": S.iso(row.expires_at)}


@router.get("/tokens")
def list_tokens(db: DB, user: CurrentUser) -> list[dict[str, Any]]:
    return [
        {"id": row.id, "label": row.label, "expires_at": S.iso(row.expires_at)}
        for row in db.scalars(select(ResearchToken).where(ResearchToken.user_id == user.id))
    ]


@router.delete("/tokens/{token_id}", status_code=204)
def revoke_token(token_id: str, db: DB, user: CurrentUser) -> None:
    row = db.get(ResearchToken, token_id)
    if row is None or row.user_id != user.id:
        raise HTTPException(404, "research token not found")
    db.delete(row)
    ops.log_event(db, "info", "research_token_revoked", token_id, user_id=user.id)


@router.get("/capabilities")
def capabilities(user: ResearchUser) -> dict[str, Any]:
    return {
        "strategies": describe(),
        "max_candidates": MAX_CANDIDATES,
        "max_windows_per_split": MAX_WINDOWS,
        "max_active_simulations": MAX_ACTIVE,
        "paper_requires_human_approval": True,
        "live_trading": False,
        "engine_fingerprint": engine_fingerprint(),
        "objective": "Net P&L after fees, drawdown and robustness on chronological held-out windows.",
        "workflow": "Save hypothesis and criteria before testing; train, then test unchanged candidates. "
        "Repeated selection on test data invalidates its holdout. No edge is a valid result.",
    }


@router.get("/windows")
def list_windows(
    request: Request,
    user: ResearchUser,
    series: Literal["KXBTC15M", "KXETH15M"] | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = Query(100, ge=1, le=500),
) -> list[dict[str, Any]]:
    rows = _source_call(ctx_of(request).source.list_windows, series=series, start=start, end=end, limit=limit)
    return [S.window_json(row) for row in rows]


@router.get("/windows/{window_id}/overview")
def overview(
    window_id: str,
    request: Request,
    user: ResearchUser,
    offset: int = Query(0, ge=0),
    limit: int = Query(60, ge=1, le=200),
) -> dict[str, Any]:
    from kmb.backend.app import get_overview

    full = get_overview(window_id, request, user)
    sections = ("series", "trades", "depth", "lifecycle")
    return {
        **{key: value for key, value in full.items() if key not in sections},
        **{key: full[key][offset : offset + limit] for key in sections},
        "offset": offset,
        "counts": {key: len(full[key]) for key in sections},
    }


@router.post("/experiments", status_code=201)
def create_experiment(
    body: ExperimentBody,
    request: Request,
    db: DB,
    user: ResearchUser,
) -> dict[str, Any]:
    if set(body.train_windows) & set(body.test_windows):
        raise HTTPException(422, "training and test windows must not overlap")
    candidates = [_candidate(candidate) for candidate in body.candidates]
    train, test = _windows(request, body.train_windows), _windows(request, body.test_windows)
    if max(_date(row["close_time"]) for row in train) > min(_date(row["open_time"]) for row in test):
        raise HTTPException(422, "test windows must follow all training windows")
    for candidate in candidates:
        for split in (train, test):
            if not any(row["series_ticker"] in candidate["series"] for row in split):
                raise HTTPException(422, f"candidate {candidate['label']} has no windows in a split")
    plan = {
        "candidates": candidates,
        "train_windows": train,
        "test_windows": test,
        "engine_fingerprint": engine_fingerprint(),
        "seed": 0,
    }
    row = ResearchExperiment(
        user_id=user.id,
        name=body.name,
        hypothesis=body.hypothesis,
        success_criteria=body.success_criteria,
        plan=plan,
    )
    db.add(row)
    db.flush()
    ops.log_event(db, "info", "research_experiment_created", body.name, user_id=user.id)
    return _experiment_json(row)


@router.get("/experiments")
def list_experiments(
    db: DB,
    user: ResearchUser,
    limit: int = Query(50, ge=1, le=100),
) -> list[dict[str, Any]]:
    return [
        {"id": row.id, "name": row.name, "hypothesis": row.hypothesis, "created_at": S.iso(row.created_at)}
        for row in db.scalars(
            select(ResearchExperiment)
            .where(
                ResearchExperiment.user_id == user.id,
            )
            .order_by(ResearchExperiment.id.desc())
            .limit(limit)
        )
    ]


@router.get("/experiments/{experiment_id}")
def get_experiment(
    experiment_id: int,
    db: DB,
    user: ResearchUser,
    include_manifest: bool = False,
    notes_offset: int = Query(0, ge=0),
    notes_limit: int = Query(5, ge=1, le=20),
) -> dict[str, Any]:
    return _experiment_json(_experiment(db, user, experiment_id), include_manifest, notes_offset, notes_limit)


@router.post("/experiments/{experiment_id}/notes")
def add_note(experiment_id: int, body: NoteBody, db: DB, user: ResearchUser) -> dict[str, Any]:
    _lock_user(db, user)
    row = _experiment(db, user, experiment_id)
    if len(row.notes) >= 100:
        raise HTTPException(409, "experiment note limit reached")
    row.notes = [*row.notes, {"text": body.text, "at": S.iso(utcnow())}]
    return _experiment_json(row)


def _budget(db: Session, user: User, count: int, kind: str = "sim") -> None:
    active = (
        db.scalar(
            select(func.count(Job.id)).where(
                Job.user_id == user.id,
                Job.kind == kind,
                Job.status.in_(ACTIVE),
            )
        )
        or 0
    )
    maximum = MAX_ACTIVE if kind == "sim" else MAX_PAPER
    if active + count > maximum:
        raise HTTPException(409, f"active {kind} job budget exceeded ({maximum})")


def _trials(db: Session, experiment_id: int, split: str | None = None) -> list[ResearchTrial]:
    query = select(ResearchTrial).where(ResearchTrial.experiment_id == experiment_id)
    if split:
        query = query.where(ResearchTrial.split == split)
    return list(db.scalars(query.order_by(ResearchTrial.id)))


def _trial_job(db: Session, trial: ResearchTrial) -> Job:
    job = db.get(Job, trial.job_id)
    if job is None:
        raise HTTPException(409, "research trial job is missing")
    return job


def _research_job_out(db: Session, job: Job) -> dict[str, Any]:
    out = _job_out(db, job)
    out.pop("spec")
    research = job.spec.get("research", {})
    out["research"] = {key: research[key] for key in ("experiment_id", "candidate", "split")}
    return out


def _job_spec(row: ResearchExperiment, index: int, split: str) -> dict[str, Any]:
    candidate = row.plan["candidates"][index]
    return {
        "bot": {
            "name": candidate["label"],
            "strategy": candidate["strategy"],
            "params": candidate["params"],
            "series": candidate["series"],
            "key_id": None,
        },
        "sim": candidate["sim"] | {"seed": row.plan["seed"]},
        "windows": [
            window["window_id"]
            for window in row.plan[f"{split}_windows"]
            if window["series_ticker"] in candidate["series"]
        ],
        "research": {
            "experiment_id": row.id,
            "candidate": index,
            "split": split,
            "engine_fingerprint": row.plan["engine_fingerprint"],
            "window_manifest": [
                window
                for window in row.plan[f"{split}_windows"]
                if window["series_ticker"] in candidate["series"]
            ],
        },
    }


@router.post("/experiments/{experiment_id}/batches", status_code=201)
def submit_batch(experiment_id: int, body: BatchBody, db: DB, user: ResearchUser) -> dict[str, Any]:
    _lock_user(db, user)
    row = _experiment(db, user, experiment_id)
    if row.plan["engine_fingerprint"] != engine_fingerprint():
        raise HTTPException(409, "engine changed; create a new experiment")
    existing = _trials(db, row.id, body.split)
    if existing:
        return {
            "split": body.split,
            "jobs": [_research_job_out(db, _trial_job(db, trial)) for trial in existing],
        }
    if body.split == "test":
        train = _trials(db, row.id, "train")
        if len(train) != len(row.plan["candidates"]) or any(
            _trial_job(db, trial).status != "succeeded" for trial in train
        ):
            raise HTTPException(409, "complete the training batch before submitting held-out tests")
    _budget(db, user, len(row.plan["candidates"]))
    jobs = []
    for index in range(len(row.plan["candidates"])):
        job = Job(user_id=user.id, kind="sim", spec=_job_spec(row, index, body.split))
        db.add(job)
        db.flush()
        db.add(ResearchTrial(experiment_id=row.id, candidate=index, split=body.split, job_id=job.id))
        ops.log_event(
            db,
            "info",
            "research_sim_created",
            f"experiment {row.id}/{body.split}/{index}",
            user_id=user.id,
            job_id=job.id,
        )
        jobs.append(_research_job_out(db, job))
    return {"split": body.split, "jobs": jobs}


@router.get("/experiments/{experiment_id}/results")
def results(experiment_id: int, db: DB, user: ResearchUser) -> dict[str, Any]:
    row = _experiment(db, user, experiment_id)
    trials = []
    for trial in _trials(db, row.id):
        job = _trial_job(db, trial)
        run = db.scalar(select(Run).where(Run.job_id == job.id))
        summary = {
            key: value
            for key, value in (run.summary if run else {}).items()
            if key not in ("equity_curve", "windows")
        }
        window_results = run.summary.get("windows", []) if run else []
        valid = (
            job.status == "succeeded"
            and run is not None
            and run.status == "succeeded"
            and len(window_results) == len(run.windows) > 0
            and {window.get("window_id") for window in window_results} == set(run.windows)
            and all(not window.get("error") and "pnl" in window for window in window_results)
        )
        window_stats = None
        if valid:
            pnls = [Decimal(window["pnl"]) for window in window_results]
            window_stats = {
                "positive_windows": sum(pnl > 0 for pnl in pnls),
                "negative_windows": sum(pnl < 0 for pnl in pnls),
                "flat_windows": sum(pnl == 0 for pnl in pnls),
                "traded_windows": sum(window["n_fills"] > 0 for window in window_results),
                "worst_window_pnl": str(min(pnls)),
                "best_window_pnl": str(max(pnls)),
                "mean_window_pnl": str(sum(pnls) / len(pnls)),
            }
        trials.append(
            {
                "candidate": trial.candidate,
                "label": row.plan["candidates"][trial.candidate]["label"],
                "split": trial.split,
                "job": _research_job_out(db, job),
                "summary": summary,
                "valid_for_comparison": valid,
                "window_stats": window_stats,
                "comparison_issue": None if valid else "Run not successful or window results incomplete.",
            }
        )
    return {
        "experiment_id": row.id,
        "trials": trials,
        "warning": "Compare like-for-like windows and costs; no trades is not evidence of an edge. "
        "Repeated tuning against test results contaminates the holdout.",
    }


def _research_job(db: Session, user: User, job_id: int) -> Job:
    job = db.get(Job, job_id)
    if (
        job is None
        or job.user_id != user.id
        or job.kind not in ("sim", "paper")
        or not job.spec.get("research")
    ):
        raise HTTPException(404, "research job not found")
    return job


@router.get("/jobs/{job_id}")
def get_job(job_id: int, db: DB, user: ResearchUser) -> dict[str, Any]:
    return _research_job_out(db, _research_job(db, user, job_id))


@router.post("/jobs/{job_id}/stop")
def stop_job(job_id: int, db: DB, user: ResearchUser) -> dict[str, Any]:
    job = _research_job(db, user, job_id)
    if job.status == "queued":
        job.status, job.finished_at = "canceled", utcnow()
    if job.status in (*ACTIVE, "canceled"):
        job.cancel_requested = True
    ops.log_event(db, "info", "research_stop_requested", str(job.id), user_id=user.id, job_id=job.id)
    return _research_job_out(db, job)


@router.get("/jobs/{job_id}/run")
def get_run(
    job_id: int,
    db: DB,
    user: ResearchUser,
    section: Literal["summary", "windows", "orders", "fills"] = "summary",
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
) -> dict[str, Any]:
    job = _research_job(db, user, job_id)
    run = db.scalar(select(Run).where(Run.job_id == job.id))
    if run is None:
        raise HTTPException(409, "run has not started")
    if section == "summary":
        out = S.run_json(run)
        out["summary"] = {
            key: value for key, value in run.summary.items() if key not in ("equity_curve", "windows")
        }
        return out
    if section == "windows":
        items = run.summary.get("windows", [])[offset : offset + limit]
    else:
        if section == "orders":
            items = [
                S.order_json(item)
                for item in db.scalars(
                    select(Order)
                    .where(
                        Order.run_id == run.id,
                    )
                    .order_by(Order.id)
                    .offset(offset)
                    .limit(limit)
                )
            ]
        else:
            items = [
                S.fill_json(item)
                for item in db.scalars(
                    select(FillRow)
                    .where(
                        FillRow.run_id == run.id,
                    )
                    .order_by(FillRow.id)
                    .offset(offset)
                    .limit(limit)
                )
            ]
    return {"run_id": run.id, "section": section, "offset": offset, "items": items}


def _proposal_json(row: ResearchPaperProposal) -> dict[str, Any]:
    return {
        "id": row.id,
        "experiment_id": row.experiment_id,
        "candidate": row.candidate,
        "rationale": row.rationale,
        "spec": row.spec,
        "status": row.status,
        "job_id": row.job_id,
        "created_at": S.iso(row.created_at),
        "decided_at": S.iso(row.decided_at),
    }


@router.post("/experiments/{experiment_id}/paper-proposals", status_code=201)
def propose_paper(experiment_id: int, body: PaperBody, db: DB, user: ResearchUser) -> dict[str, Any]:
    _lock_user(db, user)
    row = _experiment(db, user, experiment_id)
    if body.candidate >= len(row.plan["candidates"]):
        raise HTTPException(422, "candidate index is out of range")
    tested = [
        trial
        for trial in results(row.id, db, user)["trials"]
        if trial["candidate"] == body.candidate and trial["split"] == "test"
    ]
    if not tested or not tested[0]["valid_for_comparison"]:
        raise HTTPException(409, "candidate needs a successful held-out simulation first")
    pending = (
        db.scalar(
            select(func.count(ResearchPaperProposal.id))
            .join(
                ResearchExperiment,
                ResearchExperiment.id == ResearchPaperProposal.experiment_id,
            )
            .where(ResearchExperiment.user_id == user.id, ResearchPaperProposal.status == "pending")
        )
        or 0
    )
    if pending >= 10:
        raise HTTPException(409, "pending paper proposal budget exceeded (10)")
    spec = _job_spec(row, body.candidate, "test")
    spec.pop("windows")
    spec["research"] = spec["research"] | {"split": "paper"}
    spec["research"].pop("window_manifest")
    spec["duration_seconds"] = body.duration_minutes * 60
    spec["risk_limits"] = Limits.from_row(ops.risk_limits_for(db, user.id)).to_dict()
    proposal = ResearchPaperProposal(
        experiment_id=row.id,
        candidate=body.candidate,
        rationale=body.rationale,
        spec=spec,
    )
    db.add(proposal)
    db.flush()
    ops.log_event(db, "info", "research_paper_proposed", str(proposal.id), user_id=user.id)
    return _proposal_json(proposal)


@router.get("/paper-proposals")
def list_proposals(db: DB, user: ResearchUser, limit: int = Query(50, ge=1, le=100)) -> list[dict[str, Any]]:
    rows = db.scalars(
        select(ResearchPaperProposal)
        .join(
            ResearchExperiment,
            ResearchExperiment.id == ResearchPaperProposal.experiment_id,
        )
        .where(ResearchExperiment.user_id == user.id)
        .order_by(ResearchPaperProposal.id.desc())
        .limit(limit)
    )
    return [_proposal_json(row) for row in rows]


@router.post("/paper-proposals/{proposal_id}/{decision}")
def decide_paper(
    proposal_id: int,
    decision: Literal["approve", "reject"],
    db: DB,
    user: CurrentUser,
) -> dict[str, Any]:
    _lock_user(db, user)
    proposal = db.get(ResearchPaperProposal, proposal_id)
    if proposal is None:
        raise HTTPException(404, "paper proposal not found")
    _experiment(db, user, proposal.experiment_id)
    if proposal.status != "pending":
        raise HTTPException(409, "paper proposal has already been decided")
    if decision == "approve":
        if proposal.spec["research"]["engine_fingerprint"] != engine_fingerprint():
            raise HTTPException(409, "engine changed; create and test a new experiment")
        _budget(db, user, 1, "paper")
        kill = ops.kill_switch_state(db, user.id)
        if kill["global"] or kill["user"]:
            raise HTTPException(403, "a kill switch is engaged")
        job = Job(user_id=user.id, kind="paper", spec=json.loads(json.dumps(proposal.spec)))
        db.add(job)
        db.flush()
        proposal.job_id = job.id
    proposal.status = "approved" if decision == "approve" else "rejected"
    proposal.decided_at = utcnow()
    ops.log_event(
        db,
        "info",
        f"research_paper_{proposal.status}",
        str(proposal.id),
        user_id=user.id,
        job_id=proposal.job_id,
    )
    return _proposal_json(proposal)
