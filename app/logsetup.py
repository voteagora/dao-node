import logging
import time

def get_logger(name: str):
    logger = logging.getLogger(name)
    if not logger.handlers:  # prevent adding multiple handlers
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    logger.propagate = False  # Don't double-log via root
    return logger
