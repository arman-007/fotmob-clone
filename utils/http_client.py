"""
Shared HTTP Client Utilities

Provides common HTTP headers and request helpers for FotMob API interactions.
"""

import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Default timeout for API requests (seconds)
DEFAULT_TIMEOUT = 15


def get_fotmob_headers(x_mas: str) -> dict:
    """
    Build standard headers for FotMob API requests.

    Args:
        x_mas: X-MAS authentication token

    Returns:
        Headers dict ready for requests.get()
    """
    return {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.fotmob.com/',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-mas': x_mas,
    }


def create_retry_session(
    retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple = (500, 502, 503, 504),
) -> requests.Session:
    """
    Create a requests Session with automatic retry logic.

    Args:
        retries: Number of retries for failed requests
        backoff_factor: Backoff factor (1.0 = 1s, 2s, 4s delays)
        status_forcelist: HTTP status codes to retry on

    Returns:
        Configured requests.Session
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session
