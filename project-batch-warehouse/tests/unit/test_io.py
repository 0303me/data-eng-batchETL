from unittest.mock import patch

from common.io_s3 import put_bytes


def test_put_bytes_mocks_s3() -> None:
    with patch("common.io_s3.s3.put_object") as m:
        put_bytes("k", b"x")
        m.assert_called()
