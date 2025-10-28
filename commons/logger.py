import logging
import sys
from datetime import datetime

def setup_logger(name: str, level=logging.INFO):
    """
    Sets up a logger with the given name and level.
    Logs to console with a custom format including timestamp.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding handlers if already added
    if not logger.handlers:
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)

        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(handler)

    return logger