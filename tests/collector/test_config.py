from __future__ import annotations

from kmb.collector.config import CollectorConfig


def test_config_defaults_and_environment_overrides(tmp_path) -> None:
    config = CollectorConfig.from_env(
        {
            "KMB_COLLECTOR_INGEST_TOKEN": "t" * 32,
            "KMB_COLLECTOR_SPOOL_DIR": str(tmp_path),
            "KMB_COLLECTOR_KALSHI_ENV": "demo",
            "KMB_COLLECTOR_NATS_URL": "",
        }
    )
    config.check()
    assert config.series == ("KXBTC15M", "KXETH15M")
    assert config.environment == "demo"
    assert config.spool_dir == tmp_path
    assert config.nats_url == ""
    assert config.batch_max_bytes == 2 * 1024**2
    assert config.batch_max_age_seconds == 60
