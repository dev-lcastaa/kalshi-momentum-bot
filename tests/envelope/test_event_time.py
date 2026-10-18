import random
from datetime import UTC, datetime

from kmb.envelope.event import ms_to_utc_iso


def test_ms_to_utc_iso_matches_reference_formatting() -> None:
    def reference(ms: int) -> str:
        return (
            datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        )

    rng = random.Random(7)
    samples = [0, 1, 999, 1000, 1767285900000, 1767285900999]
    samples += [rng.randint(0, 4102444800000) for _ in range(5000)]
    assert all(ms_to_utc_iso(ms) == reference(ms) for ms in samples)
