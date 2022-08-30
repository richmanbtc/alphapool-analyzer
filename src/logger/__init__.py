import logging
from logging import getLogger, StreamHandler
from types import SimpleNamespace

logger_initialized = False  # TODO: refactor


def create_logger(log_level):
    global logger_initialized

    logger = getLogger("alphapool-portfolio")

    if logger_initialized:
        return logger

    level = getattr(logging, log_level.upper())
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger.setLevel(level)
    logger.propagate = False

    err = StreamHandler()
    err.setLevel(level)
    err.setFormatter(formatter)
    logger.addHandler(err)

    logger_initialized = True

    return logger


def create_null_logger():
    return SimpleNamespace(
        debug=_null_logger_func,
        error=_null_logger_func,
        warn=_null_logger_func,
        info=_null_logger_func,
    )


def _null_logger_func(x):
    ...
