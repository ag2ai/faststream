import logging

from faststream._internal.log.formatter import ColourizedFormatter


def test_formatter() -> None:
    logger = logging.getLogger(__name__)
    handler = logging.Handler()
    formatter = ColourizedFormatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info("Hi")
