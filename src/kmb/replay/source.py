"""Where windows come from: node001's data API (production) or a local derived directory (dev/tests)."""

from __future__ import annotations

import io
import json
import tempfile
import threading
from collections import OrderedDict
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import httpx

from kmb.replay.events import iter_parquet_events
from kmb.strategy.base import Event


class WindowNotFound(KeyError):
    pass


class WindowSource(Protocol):
    def list_windows(
        self, series: str | None = None, start: str | None = None, end: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]: ...
    def get_window(self, window_id: str) -> dict[str, Any]: ...
    def load_events(self, window_id: str) -> list[Event]: ...
    def iter_events(self, window_id: str) -> Iterator[Event]: ...
    def health(self) -> dict[str, Any]: ...


def _events_from_parquet_bytes(data: bytes) -> list[Event]:
    events = list(iter_parquet_events(io.BytesIO(data), ordered=False))
    events.sort(key=lambda event: event.local_seq)
    return events


def _in_range(w: dict[str, Any], series: str | None, start: str | None, end: str | None) -> bool:
    if series and w.get("series_ticker") != series:
        return False
    ot = w.get("open_time") or ""
    try:
        o = datetime.fromisoformat(ot.replace("Z", "+00:00"))
        if start and o < datetime.fromisoformat(start.replace("Z", "+00:00")):
            return False
        if end and o > datetime.fromisoformat(end.replace("Z", "+00:00")):
            return False
    except ValueError:
        return not (start or end)
    return True


class _EventCache:
    def __init__(self, max_windows: int):
        self.max = max_windows
        self._d: OrderedDict[str, list[Event]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> list[Event] | None:
        with self._lock:
            v = self._d.get(key)
            if v is not None:
                self._d.move_to_end(key)
            return v

    def put(self, key: str, value: list[Event]) -> None:
        with self._lock:
            self._d[key] = value
            self._d.move_to_end(key)
            while len(self._d) > self.max:
                self._d.popitem(last=False)


class LocalWindowSource:
    """Reads ``*.window.json`` + Parquet from a derived root (same layout as node001)."""

    def __init__(self, root: str | Path, cache_windows: int = 2):
        self.root = Path(root)
        self._cache = _EventCache(cache_windows)

    def _all(self) -> list[dict[str, Any]]:
        out = []
        for p in self.root.glob("windows/*/*/*.window.json"):
            try:
                out.append(json.loads(p.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                continue
        out.sort(key=lambda w: w.get("open_ms") or 0, reverse=True)
        return out

    def list_windows(self, series=None, start=None, end=None, limit=100):
        return [w for w in self._all() if _in_range(w, series, start, end)][:limit]

    def get_window(self, window_id: str) -> dict[str, Any]:
        for w in self._all():
            if w.get("window_id") == window_id:
                return w
        raise WindowNotFound(window_id)

    def load_events(self, window_id: str) -> list[Event]:
        cached = self._cache.get(window_id)
        if cached is not None:
            return cached
        w = self.get_window(window_id)
        events = _events_from_parquet_bytes((self.root / w["parquet"]).read_bytes())
        self._cache.put(window_id, events)
        return events

    def health(self) -> dict[str, Any]:
        return {"ok": self.root.exists(), "detail": str(self.root)}

    def iter_events(self, window_id: str) -> Iterator[Event]:
        window = self.get_window(window_id)
        yield from iter_parquet_events(self.root / window["parquet"])


class DataApiWindowSource:
    """Client for node001's read-only data API (docs/data-formats.md)."""

    def __init__(
        self,
        base_url: str,
        token: str | None,
        cache_windows: int = 2,
        timeout_s: float = 60.0,
        transport: httpx.BaseTransport | None = None,
    ):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"), headers=headers, timeout=timeout_s, transport=transport
        )
        self._cache = _EventCache(cache_windows)

    def list_windows(self, series=None, start=None, end=None, limit=100):
        params: dict[str, Any] = {"limit": limit}
        if series:
            params["series"] = series
        if start:
            params["from"] = start
        if end:
            params["to"] = end
        r = self._client.get("/v1/windows", params=params)
        r.raise_for_status()
        return r.json()

    def get_window(self, window_id: str) -> dict[str, Any]:
        r = self._client.get(f"/v1/windows/{window_id}")
        if r.status_code == 404:
            raise WindowNotFound(window_id)
        r.raise_for_status()
        return r.json()

    def load_events(self, window_id: str) -> list[Event]:
        cached = self._cache.get(window_id)
        if cached is not None:
            return cached
        r = self._client.get(f"/v1/windows/{window_id}/parquet")
        if r.status_code == 404:
            raise WindowNotFound(window_id)
        r.raise_for_status()
        events = _events_from_parquet_bytes(r.content)
        self._cache.put(window_id, events)
        return events

    def health(self) -> dict[str, Any]:
        try:
            r = self._client.get("/health", timeout=5.0)
            return {"ok": r.status_code == 200, "detail": r.json() if r.status_code == 200 else r.text[:200]}
        except httpx.HTTPError as e:
            return {"ok": False, "detail": str(e)}

    def iter_events(self, window_id: str) -> Iterator[Event]:
        # Parquet needs seeking; spool compressed bytes rather than retaining parsed events.
        with tempfile.TemporaryFile() as file:
            with self._client.stream("GET", f"/v1/windows/{window_id}/parquet") as response:
                if response.status_code == 404:
                    raise WindowNotFound(window_id)
                response.raise_for_status()
                for chunk in response.iter_bytes(chunk_size=65536):
                    file.write(chunk)
            file.seek(0)
            yield from iter_parquet_events(file)


def source_from_env() -> WindowSource:
    import os

    local = os.environ.get("KMB_WINDOWS_DIR")
    if local:
        return LocalWindowSource(local)
    url = os.environ.get("KMB_DATA_API_URL", "http://192.168.1.129:8702")
    token = os.environ.get("KMB_DATA_API_TOKEN")
    token_file = os.environ.get("KMB_DATA_API_TOKEN_FILE", "/run/secrets/data_api_token")
    if not token and os.path.exists(token_file):
        token = Path(token_file).read_text(encoding="utf-8").strip()
    return DataApiWindowSource(url, token, cache_windows=int(os.environ.get("KMB_WINDOW_CACHE", "2")))
