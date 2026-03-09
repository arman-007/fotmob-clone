"""
Match Stats Processor Module

Shared module for processing match statistics from FotMob API.
Used by both the historical pipeline and daily pipeline.

This module contains:
- API request functions
- Data processing/transformation functions
- MongoDB save operations with safety checks

The processing logic is shared to ensure consistency between pipelines.

NOTE: All IDs (match_id, league_id, team_id, player_id) are stored as INTEGERS
in MongoDB for consistency and query performance.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple, Union

import requests
from dotenv import load_dotenv

from get_additional_stats import process_additional_stats

# MongoDB imports - wrapped in try/except for when running without MongoDB
try:
    from db import (
        get_mongodb_service,
        parse_datetime,
        safe_int,
        safe_float
    )
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    
    def parse_datetime(dt_string):
        if not dt_string:
            return None
        for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"]:
            try:
                return datetime.strptime(dt_string, fmt)
            except ValueError:
                continue
        return None
    
    def safe_int(value, default=None):
        """Safely convert value to int."""
        if value is None:
            return default
        if isinstance(value, int):
            return value
        try:
            return int(str(value).strip())
        except (ValueError, TypeError):
            return default
    
    def safe_float(value, default=None):
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

load_dotenv()

logger = logging.getLogger(__name__)

URL = os.environ.get("URL")


from service.auth_utils import get_auth_headers, generate_x_mas_header
from service.playwright_auth import fetch_json_playwright

# =============================================================================
# API Request Functions
# =============================================================================

import re

def fetch_match_details(match_id: Union[int, str]) -> Optional[dict]:
    """
    Fetch match details by extracting the Next.js state from the match page HTML.
    This completely bypasses the /api/data/matchDetails Cloudflare protection.
    Falls back to Playwright if the HTML extraction fails.
    """
    url = f"https://www.fotmob.com/match/{match_id}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    try:
        logger.info(f"Extracting Next.js data from HTML: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text)
        if match:
            data = json.loads(match.group(1))
            if 'props' in data and 'pageProps' in data['props']:
                # The pageProps contains the exact same structure as the API response!
                page_props = data['props']['pageProps']
                if 'general' in page_props:
                    logger.info(f"✅ Successfully extracted NEXT_DATA for match {match_id}")
                    return page_props
        
        logger.warning(f"__NEXT_DATA__ not found or missing 'general' key for match {match_id}. HTML size: {len(response.text)}")
    except Exception as e:
        logger.warning(f"HTML extraction failed for match {match_id}: {e}")
        
    logger.info("Falling back to Playwright API fetch...")
    if not URL:
        logger.error("URL environment variable is not set")
        return None
        
    api_path = f"/api/data/matchDetails?matchId={match_id}"
    api_url = f"https://www.fotmob.com{api_path}"
    
    try:
        headers_pw = {"x-mas": generate_x_mas_header(api_path)}
        data = fetch_json_playwright(api_url, headers=headers_pw)
        if data:
            return data
            
    except Exception as e:
        logger.error(f"❌ Error fetching match details for {match_id} via Playwright: {e}")
        
    return None


# =============================================================================
# Data Processing Functions
# =============================================================================

def process_general_section(general_data: dict) -> dict:
    """
    Process the general section of match data.
    
    Args:
        general_data: The 'general' section from API response
        
    Returns:
        Processed match info dict with integer IDs
    """
    home_team = general_data.get("homeTeam", {})
    away_team = general_data.get("awayTeam", {})
    
    return {
        "match_id": safe_int(general_data.get("matchId")),
        "match_name": general_data.get("matchName"),
        "league_id": safe_int(general_data.get("parentLeagueId")),
        "league_name": general_data.get("leagueName"),
        "match_date_time_UTC": general_data.get("matchTimeUTCDate"),
        "started": general_data.get("started"),
        "finished": general_data.get("finished"),
        "home_team_id": safe_int(home_team.get("id")),
        "home_team_name": home_team.get("name"),
        "away_team_id": safe_int(away_team.get("id")),
        "away_team_name": away_team.get("name"),
    }


def process_player_stats_detail(raw_stats_list: list) -> dict:
    """
    Process raw player stats into simplified format.
    
    Args:
        raw_stats_list: List of stat category items from API
        
    Returns:
        Simplified stats dict
    """
    simplified_stats = {}

    if not isinstance(raw_stats_list, list) or not raw_stats_list:
        return simplified_stats

    for stat_category_item in raw_stats_list:
        all_stats_data = stat_category_item.get("stats", {})

        for stat_name, stat_object in all_stats_data.items():
            stat_key = stat_object.get("key") or stat_name
            stat_value_data = stat_object.get("stat", {})
            stat_type = stat_value_data.get("type")
            value = stat_value_data.get("value")
            total = stat_value_data.get("total")
            
            if stat_type in ("integer", "double", "speed", "distance"):
                simplified_stats[stat_key] = value
            elif stat_type in ("fractionWithPercentage", "distanceWithPercentage"):
                if value is not None and total is not None and total > 0:
                    percentage = (value / total) * 100
                    simplified_stats[stat_key] = f"{value}/{total} ({percentage:.1f}%)"
                elif value is not None:
                    simplified_stats[stat_key] = f"{value}/N/A"
                else:
                    simplified_stats[stat_key] = None
            elif stat_type == "boolean":
                simplified_stats[stat_key] = bool(value) if value is not None else False
            else:
                simplified_stats[stat_key] = value if value is not None else None
    
    return simplified_stats


def process_individual_player(player_stats: dict) -> dict:
    """
    Process individual player statistics.
    
    Args:
        player_stats: Player stats dict from API
        
    Returns:
        Processed player stats dict with integer IDs
    """
    stats = {
        "name": player_stats.get("name"),
        "player_fotmob_id": safe_int(player_stats.get("id")),
        "player_team_id": safe_int(player_stats.get("teamId")),
        "player_team_name": player_stats.get("teamName"),
        "isGoalkeeper": player_stats.get("isGoalkeeper"),
    }

    stats_for_processing = player_stats.get("stats")
    simplified_stats = process_player_stats_detail(stats_for_processing)
    stats.update(simplified_stats)
    
    return stats


def process_content_section(content_data: dict) -> List[dict]:
    """
    Process content section to extract player stats.
    
    Args:
        content_data: The 'content' section from API response
        
    Returns:
        List of player stats dicts
    """
    if not content_data:
        logger.debug("process_content_section received empty content_data")
        return []
    
    if content_data.get("playerStats") is None:
        logger.debug("playerStats is None (match may not have started)")
        return []
    
    player_stats_data = content_data.get("playerStats", {})
    player_stats = []

    for player_id in player_stats_data.keys():
        individual_stats = process_individual_player(
            player_stats_data.get(player_id, {})
        )
        player_stats.append(individual_stats)

    # Get additional stats (yellow cards, goals, etc.)
    additional_stats = process_additional_stats(content_data.get("matchFacts", {}))
    # Convert keys to int for matching
    additional_stats = {safe_int(k): v for k, v in additional_stats.items()}

    # Merge additional stats into player stats
    for player in player_stats:
        player_id = player.get("player_fotmob_id")
        if player_id and player_id in additional_stats:
            for new_stats in additional_stats[player_id]:
                player.update(new_stats)
    
    return player_stats


def process_match_response(response_data: Optional[dict]) -> dict:
    """
    Process complete match information from API response.
    
    Args:
        response_data: Raw API response
        
    Returns:
        Processed match info dict with player_stats if match started
    """
    if response_data is None:
        logger.warning("process_match_response received None response_data")
        return {}

    processed_info = process_general_section(response_data.get("general", {}))

    # Only process player stats if match has started
    if processed_info.get("started"):
        player_stats = process_content_section(response_data.get("content", {}))
        processed_info["player_stats"] = player_stats

    return processed_info


# =============================================================================
# MongoDB Save Operations (with Safety Checks)
# =============================================================================

def save_match_to_mongodb(
    match_data: dict,
    league_id: Union[int, str] = None,
    season_id: str = None,
    safe_update: bool = True
) -> Tuple[bool, Dict[str, int]]:
    """
    Save match data to MongoDB (both matches and player_stats collections).
    
    Implements SAFE UPDATE logic:
    - Won't overwrite existing player_stats with empty data
    - Only updates fields that have meaningful values
    - Preserves existing data if new data is incomplete
    
    All IDs are stored as integers for consistency.
    
    Args:
        match_data: Processed match data dictionary
        league_id: Optional league ID (will extract from match_data if not provided)
        season_id: Optional season ID (for historical pipeline compatibility)
        safe_update: If True, applies safety checks before updating
        
    Returns:
        Tuple of (success, stats_dict)
    """
    stats = {"matches": 0, "player_stats": 0, "teams": 0, "errors": 0, "skipped": 0}
    
    if not MONGODB_AVAILABLE:
        logger.warning("MongoDB not available, skipping save")
        return False, stats
    
    mongo = get_mongodb_service()
    
    if not match_data or not match_data.get("match_id"):
        logger.warning("Empty or invalid match data, skipping MongoDB save")
        return False, stats
    
    # Convert all IDs to integers
    match_id = safe_int(match_data["match_id"])
    if not match_id:
        logger.warning(f"Invalid match_id: {match_data.get('match_id')}")
        return False, stats
    
    # Extract or use provided league/season info
    league_id = safe_int(league_id) or safe_int(match_data.get("league_id"))
    
    # For daily pipeline, we may not have season_id - try to determine it
    if not season_id:
        season_id = _determine_season_from_date(match_data.get("match_date_time_UTC"))
    
    league_season_key = f"{league_id}_{season_id}" if league_id and season_id else ""
    
    match_datetime = parse_datetime(match_data.get("match_date_time_UTC"))
    
    # Extract team info as integers
    home_team_id = safe_int(match_data.get("home_team_id"))
    away_team_id = safe_int(match_data.get("away_team_id"))
    home_team_name = match_data.get("home_team_name", "")
    away_team_name = match_data.get("away_team_name", "")
    
    # =========================================================================
    # SAFETY CHECK: Don't update if new data is less complete than existing
    # =========================================================================
    if safe_update:
        existing_match = mongo.matches.find_one({"match_id": match_id})
        if existing_match:
            # Check if we're about to lose data
            existing_player_count = len(existing_match.get("player_stats", []))
            new_player_count = len(match_data.get("player_stats", []))
            
            # If existing has player stats but new doesn't, skip player_stats update
            if existing_player_count > 0 and new_player_count == 0:
                logger.warning(
                    f"⚠️ Safety check: Match {match_id} has {existing_player_count} existing "
                    f"player stats but new data has 0. Skipping player_stats update."
                )
                stats["skipped"] = existing_player_count
                # Still update match metadata (started, finished, etc.)
                _update_match_metadata_only(mongo, match_id, match_data)
                return True, stats
    
    # Track teams for bulk insert
    teams_to_insert = []
    if home_team_id:
        teams_to_insert.append((home_team_id, home_team_name))
    if away_team_id:
        teams_to_insert.append((away_team_id, away_team_name))
    
    # Process player stats
    raw_player_stats = match_data.get("player_stats", [])
    embedded_player_stats = []  # For matches collection
    flattened_player_stats = []  # For player_stats collection
    
    match_goals = 0
    match_yellow_cards = 0
    match_red_cards = 0
    
    for ps in raw_player_stats:
        player_id = safe_int(ps.get("player_fotmob_id"))
        if not player_id:
            continue
        
        player_team_id = safe_int(ps.get("player_team_id"))
        is_home = player_team_id == home_team_id
        opponent_team_id = away_team_id if is_home else home_team_id
        opponent_team_name = away_team_name if is_home else home_team_name
        
        # Extract stats safely
        goals = safe_int(ps.get("goals"), 0)
        assists = safe_int(ps.get("assists"), 0)
        yellow_card = bool(ps.get("yellow_card", False))
        red_card = bool(ps.get("red_card", False))
        rating = safe_float(ps.get("rating"))
        minutes_played = safe_int(ps.get("minutes_played"), 0)
        
        # Aggregate match stats
        match_goals += goals
        match_yellow_cards += 1 if yellow_card else 0
        match_red_cards += 1 if red_card else 0
        
        # Build embedded player stat (for matches collection)
        embedded_stat = {
            "player_id": player_id,  # int
            "name": ps.get("name", ""),
            "team_id": player_team_id,  # int
            "team_name": ps.get("player_team_name", ""),
            "is_goalkeeper": ps.get("isGoalkeeper", False),
            "goals": goals,
            "assists": assists,
            "yellow_card": yellow_card,
            "red_card": red_card,
            "rating": rating,
            "minutes_played": minutes_played,
        }
        
        # Add additional stats dynamically
        excluded_keys = {
            "name", "player_fotmob_id", "player_team_id",
            "player_team_name", "isGoalkeeper", "goals",
            "assists", "yellow_card", "red_card", "rating",
            "minutes_played"
        }
        for key, value in ps.items():
            if key not in excluded_keys:
                embedded_stat[key] = value
        
        embedded_player_stats.append(embedded_stat)
        
        # Build flattened player stat (for player_stats collection)
        flattened_stat = {
            "player_match_key": f"{player_id}_{match_id}",  # string key for uniqueness
            "player_id": player_id,  # int
            "name": ps.get("name", ""),
            "team_id": player_team_id,  # int
            "team_name": ps.get("player_team_name", ""),
            "is_goalkeeper": ps.get("isGoalkeeper", False),
            "match_id": match_id,  # int
            "match_datetime_utc": match_datetime,
            "league_id": league_id,  # int
            "season_id": season_id,
            "league_season_key": league_season_key,
            "opponent_team_id": opponent_team_id,  # int
            "opponent_team_name": opponent_team_name,
            "is_home": is_home,
            "goals": goals,
            "assists": assists,
            "yellow_card": yellow_card,
            "red_card": red_card,
            "rating": rating,
            "minutes_played": minutes_played,
        }
        
        # Add additional stats
        for key, value in ps.items():
            if key not in excluded_keys:
                flattened_stat[key] = value
        
        flattened_player_stats.append(flattened_stat)
    
    # Build match document
    match_doc = {
        "match_id": match_id,  # int
        "league_id": league_id,  # int
        "season_id": season_id,
        "league_season_key": league_season_key,
        "match_name": match_data.get("match_name", ""),
        "match_datetime_utc": match_datetime,
        "started": match_data.get("started", False),
        "finished": match_data.get("finished", False),
        "home_team": {
            "team_id": home_team_id,  # int
            "name": home_team_name
        },
        "away_team": {
            "team_id": away_team_id,  # int
            "name": away_team_name
        },
        "player_stats": embedded_player_stats,
        "stats_summary": {
            "total_goals": match_goals,
            "total_yellow_cards": match_yellow_cards,
            "total_red_cards": match_red_cards
        }
    }
    
    # =========================================================================
    # Insert to MongoDB
    # =========================================================================
    try:
        # Insert match
        success, error = mongo.insert_match(match_doc, validate=True)
        if success:
            stats["matches"] = 1
        else:
            logger.warning(f"Failed to insert match {match_id}: {error}")
            stats["errors"] += 1
        
        # Bulk insert player stats
        if flattened_player_stats:
            ps_result = mongo.insert_player_stats_bulk(flattened_player_stats, validate=True)
            stats["player_stats"] = ps_result.get("inserted", 0) + ps_result.get("modified", 0)
            stats["errors"] += ps_result.get("errors", 0)
        
        # Insert teams
        if teams_to_insert:
            team_result = mongo.insert_teams_bulk(teams_to_insert)
            stats["teams"] = team_result.get("inserted", 0) + team_result.get("modified", 0)
        
        return True, stats
        
    except Exception as e:
        logger.error(f"Error saving match {match_id} to MongoDB: {e}")
        stats["errors"] += 1
        return False, stats


def _update_match_metadata_only(mongo, match_id: int, match_data: dict) -> None:
    """
    Update only match metadata (status, timestamps) without touching player_stats.
    Used when safety check prevents full update.
    """
    try:
        now = datetime.now(timezone.utc)
        mongo.matches.update_one(
            {"match_id": match_id},
            {
                "$set": {
                    "started": match_data.get("started", False),
                    "finished": match_data.get("finished", False),
                    "updated_at": now
                }
            }
        )
        logger.debug(f"Updated metadata only for match {match_id}")
    except Exception as e:
        logger.error(f"Error updating match metadata: {e}")


def _determine_season_from_date(date_string: str) -> str:
    """
    Determine season ID from match date.
    
    Football seasons typically run Aug-May, so:
    - Aug 2024 - May 2025 = "2024-2025"
    - Aug 2023 - May 2024 = "2023-2024"
    
    Args:
        date_string: ISO format date string
        
    Returns:
        Season ID string (e.g., "2024-2025")
    """
    if not date_string:
        # Default to current season
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
    
    # If month is Aug-Dec, season is year/year+1
    # If month is Jan-Jul, season is year-1/year
    if month >= 8:  # Aug-Dec
        return f"{year}-{year + 1}"
    else:  # Jan-Jul
        return f"{year - 1}-{year}"


# =============================================================================
# JSON File Operations
# =============================================================================

def save_match_to_json(
    match_data: dict,
    match_id: Union[int, str],
    output_dir: str = "output/daily/player_stats"
) -> None:
    """
    Save match/player stats to JSON file.
    
    Args:
        match_data: Processed match data
        match_id: Match ID for filename
        output_dir: Output directory
    """
    filename = f"player_stats_matchID_{match_id}.json"
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(match_data, f, indent=4, default=str)
        
        logger.debug(f"✅ Saved to {filepath}")
        
    except IOError as e:
        logger.error(f"❌ Failed to save JSON: {e}")
    except TypeError as e:
        logger.error(f"❌ Serialization error: {e}")