"""
Service Module

Contains functions for fetching data from FotMob API.

Modules:
- get_auth_headers: X-MAS token capture using Selenium
- get_leagues: Fetch all leagues
- get_specific_league: Fetch league/season data
- get_player_stats: Fetch player stats (historical pipeline)
- get_daily_matches: Fetch matches by date (daily pipeline)
- match_stats_processor: Shared processing logic for match stats
"""

from service.get_auth_headers import capture_auth_info
from service.auth_utils import get_auth_headers, set_auth_info
from service.get_leagues import get_all_leagues
from service.get_specific_league import get_specific_league_data
from service.get_player_stats import get_match_wise_player_stats

# Daily pipeline modules
from service.get_daily_matches import (
    fetch_matches_by_date,
    get_match_ids_from_json,
    get_leagues_from_matches
)

from service.match_stats_processor import (
    fetch_match_details,
    process_match_response,
    process_general_section,
    process_content_section,
    save_match_to_mongodb,
    save_match_to_json
)

__all__ = [
    # Authentication
    "capture_auth_info",
    "get_auth_headers",
    "set_auth_info",
    
    # Historical pipeline
    "get_all_leagues",
    "get_specific_league_data",
    "get_match_wise_player_stats",
    
    # Daily pipeline
    "fetch_matches_by_date",
    "get_match_ids_from_json",
    "get_leagues_from_matches",
    
    # Shared processing
    "fetch_match_details",
    "process_match_response",
    "process_general_section",
    "process_content_section",
    "save_match_to_mongodb",
    "save_match_to_json"
]