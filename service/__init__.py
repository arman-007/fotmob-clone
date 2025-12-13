"""
Service Module

Contains functions for fetching data from FotMob API.
"""

from service.get_auth_headers import capture_x_mas
from service.get_leagues import get_all_leagues
from service.get_specific_league import get_specific_league_data
from service.get_player_stats import get_match_wise_player_stats

__all__ = [
    "capture_x_mas",
    "get_all_leagues",
    "get_specific_league_data",
    "get_match_wise_player_stats"
]
