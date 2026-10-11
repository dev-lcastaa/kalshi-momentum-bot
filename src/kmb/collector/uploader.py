from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import httpx

from .spool import PendingBatch, Spool

logger = logging.getLogger(__name__)


@dataclass
class UploaderStats:
    last_success: float | None = None
    errors: int = 0
    attempts: int = 0


class BatchUploader:
    def __init__(
        self,
        spool: Spool,
        base_url: str,
        token: str,
        *,
        client: httpx.AsyncClient | None = None,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
    ) -> None:
        self.spool = spool
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.client = client or httpx.AsyncClient(timeout=20)
        self._owns_client = client is None
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.stats = UploaderStats()
        self._stop = asyncio.Event()

    async def close(self) -> None:
        self._stop.set()
        if self._owns_client:
            await self.client.aclose()

    async def run(self) -> None:
        backoff = self.initial_backoff
        while not self._stop.is_set():
            self.spool.seal_due()
            pending = self.spool.pending_batches()
            if not pending:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=1)
                except TimeoutError:
                    pass
                continue
            outcome = await self.upload_one(pending[0])
            if outcome == "retry":
                self.stats.errors += 1
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except TimeoutError:
                    pass
                backoff = min(backoff * 2, self.max_backoff)
            else:
                if outcome == "rejected":
                    self.stats.errors += 1
                backoff = self.initial_backoff

    async def upload_one(self, batch: PendingBatch) -> str:
        path = f"/v1/batches/{batch.meta.batch_id}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "X-Batch-Meta": batch.meta.to_json(),
            "Content-Type": "application/gzip",
        }
        self.stats.attempts += 1
        try:
            response = await self.client.put(
                self.base_url + path, content=batch.path.read_bytes(), headers=headers
            )
        except (httpx.HTTPError, OSError) as exc:
            logger.warning("batch upload network error %s: %s", batch.meta.batch_id, exc)
            return "retry"
        if response.status_code >= 400:
            retryable = _retryable(response)
            if retryable:
                return "retry"
            self.spool.reject(batch, f"HTTP {response.status_code}: {response.text[:1000]}")
            logger.error(
                "permanently rejected batch %s: HTTP %s %s",
                batch.meta.batch_id,
                response.status_code,
                response.text,
            )
            return "rejected"
        try:
            confirmation = await self.client.get(
                self.base_url + path,
                headers={"Authorization": f"Bearer {self.token}"},
            )
        except httpx.HTTPError as exc:
            logger.warning("batch confirmation network error %s: %s", batch.meta.batch_id, exc)
            return "retry"
        if confirmation.status_code >= 400:
            if _retryable(confirmation):
                return "retry"
            self.spool.reject(
                batch, f"confirmation HTTP {confirmation.status_code}: {confirmation.text[:1000]}"
            )
            logger.error("batch confirmation permanently failed for %s", batch.meta.batch_id)
            return "rejected"
        try:
            receipt = confirmation.json().get("receipt", {})
            receipt_status = receipt.get("status")
            sha_matches = receipt.get("sha256") == batch.meta.sha256
        except (ValueError, AttributeError):
            self.spool.reject(batch, "invalid batch confirmation response")
            logger.error("invalid batch confirmation response for %s", batch.meta.batch_id)
            return "rejected"
        if receipt_status == "pending":
            logger.warning("batch %s is not committed yet", batch.meta.batch_id)
            return "retry"
        if receipt_status != "committed":
            self.spool.reject(batch, f"unexpected receipt status: {receipt_status!r}")
            logger.error("batch %s confirmation status is %r", batch.meta.batch_id, receipt_status)
            return "rejected"
        if not sha_matches:
            self.spool.reject(batch, "committed receipt SHA-256 differs from local batch metadata")
            logger.error("committed batch SHA-256 mismatch for %s", batch.meta.batch_id)
            return "rejected"
        self.spool.confirm_deleted(batch)
        self.stats.last_success = asyncio.get_running_loop().time()
        return "uploaded"


def _retryable(response: httpx.Response) -> bool:
    try:
        return response.json().get("retryable") is True
    except (ValueError, AttributeError):
        return False
