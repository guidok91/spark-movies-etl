import logging


def _build_logger() -> logging.Logger:
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(format=log_format)

    basic_logger = logging.getLogger(__name__)
    basic_logger.setLevel(logging.INFO)

    return basic_logger


logger = _build_logger()
