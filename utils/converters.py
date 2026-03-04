"""
Shared Conversion Utilities

Provides safe type conversion functions used across the codebase.
Centralizes safe_int, safe_float, parse_datetime, and determine_season_from_date
to eliminate duplication.
"""

from datetime import datetime
from typing import Any, Optional


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert a value to integer."""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return default
        try:
            return int(value)
        except ValueError:
            return default
    return default


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely convert a value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def parse_datetime(dt_string: str) -> Optional[datetime]:
    """Parse datetime string to datetime object."""
    if not dt_string:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(dt_string, fmt)
        except ValueError:
            continue

    return None


def determine_season_from_date(date_string: str) -> str:
    """
    Determine season ID from match date.

    Football seasons typically run Aug-May, so:
    - Aug 2024 - May 2025 = "2024-2025"
    - Aug 2023 - May 2024 = "2023-2024"
    """
    if not date_string:
        now = datetime.now()
        year = now.year
        month = now.month
    else:
        dt = parse_datetime(date_string)
        if not dt:
            now = datetime.now()
            year = now.year
            month = now.month
        else:
            year = dt.year
            month = dt.month

    if month >= 8:  # Aug-Dec
        return f"{year}-{year + 1}"
    else:  # Jan-Jul
        return f"{year - 1}-{year}"
