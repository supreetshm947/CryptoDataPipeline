import logging
from logging.handlers import RotatingFileHandler
import os


def get_logger(log_file='app.log'):
    # Ensure the logs directory exists
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Path to the log file
    log_path = os.path.join(log_dir, log_file)

    # Create a custom logger
    logger = logging.getLogger(__name__)

    # If the logger already has handlers, avoid duplicating logs
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)

        # Create handlers
        console_handler = logging.StreamHandler()  # Console handler
        file_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3)  # File handler

        # Create formatters and add them to the handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
