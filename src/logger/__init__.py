import logging
import resource
import sys
from contextlib import contextmanager
from time import perf_counter


def create_logger(log_level):
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('alphapool-portfolio')
    logger.setLevel((log_level or 'INFO').upper())
    return logger


@contextmanager
def log_execution_metrics(logger):
    start = perf_counter()
    status = 'failed'
    try:
        yield
        status = 'completed'
    finally:
        elapsed = perf_counter() - start
        # ru_maxrss is bytes on macOS and KiB on Linux; it covers process lifetime.
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_mib = peak / (1024 * 1024 if sys.platform == 'darwin' else 1024)
        logger.info('Execution finished: status=%s elapsed_seconds=%.3f peak_rss_mib=%.1f',
                    status, elapsed, peak_mib)
