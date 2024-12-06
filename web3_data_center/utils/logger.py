import logging
import sys

def get_logger(name: str) -> logging.Logger:
    # Configure specific loggers to reduce noise
    logging.getLogger('web3').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.WARNING)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
    return logger