import logging


def create_logger(log_level):
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('alphapool-portfolio')
    logger.setLevel((log_level or 'INFO').upper())
    return logger
