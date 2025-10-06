from unittest.mock import MagicMock, patch

from common.ingest_tlc import ingest_month


class DummyResp:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self) -> None:
        return None


@patch("common.ingest_tlc.requests.get")
@patch("common.io_s3.s3.put_object")
def test_ingest_month_writes_manifest(mock_put: MagicMock, mock_get: MagicMock) -> None:
    import io

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    df = pd.DataFrame(
        {
            "VendorID": [1],
            "tpep_pickup_datetime": ["2021-02-01 00:00:00"],
            "tpep_dropoff_datetime": ["2021-02-01 00:10:00"],
            "PULocationID": [142],
            "DOLocationID": [236],
            "fare_amount": [10.5],
            "total_amount": [12.0],
        }
    )
    sink = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), sink)
    mock_get.return_value = DummyResp(sink.getvalue())
    key, manifest = ingest_month("2021-02")
    assert key is not None and "2021/" in key and "02/" in key
    assert manifest["rows"] == 1
