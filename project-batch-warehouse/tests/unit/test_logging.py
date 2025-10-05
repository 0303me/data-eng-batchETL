from common.logging import get_logger


def test_logger_no_throw() -> None:
    log = get_logger()
    log.info("hello", ok=True)
