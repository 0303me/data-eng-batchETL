from common.ingest_tlc import bronze_key, build_manifest


def test_bronze_key_format() -> None:
    k = bronze_key("2025-02", "deadbeefcafebabe")
    assert k.startswith("bronze/trips/year=2025/month=02/")


def test_manifest_minimal() -> None:
    m = build_manifest(
        "2025-02", "https://x/y.parquet", type("S", (), {"sha256": "s", "rows": 1, "bytes": 2})()
    )
    assert m["month"] == "2025-02"
    assert m["rows"] == 1
