"""
Shared Logging Configuration

Provides centralized logging setup used by both pipelines.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler


def setup_logging(
    log_file: str = 'logs/pipeline_log.txt',
    verbose: bool = False,
    suppress_noisy: bool = False,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Configure logging for a pipeline.

    Args:
        log_file: Path to the log file
        verbose: Enable DEBUG level logging
        suppress_noisy: Suppress verbose third-party loggers
        max_bytes: Max log file size before rotation
        backup_count: Number of backup log files to keep

    Returns:
        Logger instance
    """
    os.makedirs(os.path.dirname(log_file) if os.path.dirname(log_file) else 'logs', exist_ok=True)

    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        handlers=[
            RotatingFileHandler(
                log_file,
                mode='a',
                maxBytes=max_bytes,
                backupCount=backup_count
            ),
            logging.StreamHandler(sys.stdout)
        ],
        format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s',
    )

    if suppress_noisy:
        suppress_noisy_loggers()

    return logging.getLogger(__name__)


def suppress_noisy_loggers():
    """
    Suppress verbose logs from third-party libraries.

    Selenium-wire logs every HTTP request/response which creates
    hundreds of log lines just from loading one page.
    """
    noisy_loggers = [
        'seleniumwire.handler',
        'seleniumwire.server',
        'seleniumwire.backend',
        'seleniumwire.storage',
        'seleniumwire',
        'urllib3',
        'urllib3.connectionpool',
        'hpack',
        'selenium',
        'selenium.webdriver.remote.remote_connection',
    ]

    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
