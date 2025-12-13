"""
Get Player Stats Module

Fetches match-wise player statistics from FotMob API and stores in:
1. JSON files (for debugging/backup)
2. MongoDB - matches collection (with embedded player_stats)
3. MongoDB - player_stats collection (flattened for player queries)

The JSON saving is kept for debugging purposes but can be commented out.
"""

import json
import logging
import os
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from dotenv import load_dotenv

from service.get_auth_headers import capture_x_mas
from get_additional_stats import process_additional_stats

# MongoDB imports - wrapped in try/except for when running without MongoDB
try:
    from db import (
        get_mongodb_service,
        extract_season_from_path,
        parse_datetime,
        safe_int,
        safe_float
    )
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    # Provide fallback functions when MongoDB is not available
    def extract_season_from_path(path):
        import re
        match = re.search(r'(\d{4}-\d{2})', path)
        return match.group(1) if match else None
    
    def parse_datetime(dt_string):
        if not dt_string:
            return None
        for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"]:
            try:
                return datetime.strptime(dt_string, fmt)
            except ValueError:
                continue
        return None
    
    def safe_int(value, default=0):
        if value is None:
            return default
        try:
            return int(value)
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


# =============================================================================
# JSON File Operations (kept for debugging)
# =============================================================================

def save_data_to_json(season_file_path: str, data: dict, match_id: str) -> None:
    """
    Save player stats to JSON file.
    
    This function is kept for debugging purposes.
    Comment out the call in get_match_wise_player_stats() to skip JSON saving.
    """
    output_dir = f"{os.path.dirname(season_file_path)}/player_stats/"
    filename = f"player_stats_matchID_{match_id}.json"
    
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        logger.error(f"❌ Failed to create directory '{output_dir}': {e}")
        return

    filepath = os.path.join(output_dir, filename)

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        logger.debug(f"✅ JSON saved to: {filepath}")
    except IOError as e:
        logger.error(f"❌ Failed to write to '{filepath}': {e}")
    except TypeError as e:
        logger.error(f"❌ Serialization error for '{filename}': {e}")


# =============================================================================
# MongoDB Operations
# =============================================================================

def _save_match_to_mongodb(
    match_data: dict,
    season_file_path: str
) -> Tuple[bool, Dict[str, int]]:
    """
    Save match data to MongoDB (both matches and player_stats collections).
    
    Args:
        match_data: Processed match data dictionary
        season_file_path: Path to season file (used to extract league/season info)
        
    Returns:
        Tuple of (success, stats_dict)
    """
    stats = {"matches": 0, "player_stats": 0, "teams": 0, "errors": 0}
    
    if not MONGODB_AVAILABLE:
        logger.warning("MongoDB not available, skipping save")
        return False, stats
    
    mongo = get_mongodb_service()
    
    if not match_data or not match_data.get("match_id"):
        logger.warning("Empty or invalid match data, skipping MongoDB save")
        return False, stats
    
    # Extract league/season from path
    season_id = extract_season_from_path(season_file_path)
    league_id = str(match_data.get("league_id", ""))
    league_season_key = f"{league_id}_{season_id}" if league_id and season_id else ""
    
    match_id = str(match_data["match_id"])
    match_datetime = parse_datetime(match_data.get("match_date_time_UTC"))
    
    # Extract team info
    home_team_id = str(match_data.get("home_team_id", ""))
    away_team_id = str(match_data.get("away_team_id", ""))
    home_team_name = match_data.get("home_team_name", "")
    away_team_name = match_data.get("away_team_name", "")
    
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
        player_id = str(ps.get("player_fotmob_id", ""))
        if not player_id:
            continue
        
        player_team_id = str(ps.get("player_team_id", ""))
        is_home = player_team_id == home_team_id
        opponent_team_id = away_team_id if is_home else home_team_id
        opponent_team_name = away_team_name if is_home else home_team_name
        
        # Extract stats safely
        goals = safe_int(ps.get("goals", 0))
        assists = safe_int(ps.get("assists", 0))
        yellow_card = bool(ps.get("yellow_card", False))
        red_card = bool(ps.get("red_card", False))
        rating = safe_float(ps.get("rating"))
        minutes_played = safe_int(ps.get("minutes_played", 0))
        
        # Aggregate match stats
        match_goals += goals
        match_yellow_cards += 1 if yellow_card else 0
        match_red_cards += 1 if red_card else 0
        
        # Build embedded player stat (for matches collection)
        embedded_stat = {
            "player_id": player_id,
            "name": ps.get("name", ""),
            "team_id": player_team_id,
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
            "player_match_key": f"{player_id}_{match_id}",
            "player_id": player_id,
            "name": ps.get("name", ""),
            "team_id": player_team_id,
            "team_name": ps.get("player_team_name", ""),
            "is_goalkeeper": ps.get("isGoalkeeper", False),
            "match_id": match_id,
            "match_datetime_utc": match_datetime,
            "league_id": league_id,
            "season_id": season_id,
            "league_season_key": league_season_key,
            "opponent_team_id": opponent_team_id,
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
        "match_id": match_id,
        "league_id": league_id,
        "season_id": season_id,
        "league_season_key": league_season_key,
        "match_name": match_data.get("match_name", ""),
        "match_datetime_utc": match_datetime,
        "started": match_data.get("started", False),
        "finished": match_data.get("finished", False),
        "home_team": {
            "team_id": home_team_id,
            "name": home_team_name
        },
        "away_team": {
            "team_id": away_team_id,
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


# =============================================================================
# API Request Functions
# =============================================================================

def _fetch_match_ids(date: str) -> Optional[List[str]]:
    """Fetch match IDs from a stored JSON file by date."""
    filepath = f"output/get_matches_{date}.json"
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            logger.debug(f"📂 Successfully loaded data from {filepath}")
        
        match_ids = []
        if data and data.get("leagues"):
            for league in data.get("leagues"):
                for match in league.get("matches", []):
                    match_ids.append(match.get("id"))
            return match_ids
        else:
            logger.warning("No response or no 'leagues' data found.")
            return None
            
    except FileNotFoundError:
        logger.error(f"⚠️ File not found: {filepath}")
        return None
    except json.JSONDecodeError:
        logger.error(f"⚠️ Invalid JSON in: {filepath}")
        return None


def _make_request(x_mas: str, match_id: str) -> Optional[dict]:
    """Make API request to fetch match details."""
    url = f"{URL}/matchDetails"
    
    params = {'matchId': match_id}
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.fotmob.com/',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-mas': x_mas,
    }
    
    # Debug: log the x-mas token being used
    if x_mas:
        logger.debug(f"Using X-MAS token: {x_mas[:30]}...")
    else:
        logger.error(f"❌ X-MAS token is None or empty for match {match_id}!")
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as e:
        logger.error(f"❌ HTTP Error for match {match_id}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Network Error for match {match_id}: {e}")
        return None
    except json.JSONDecodeError:
        logger.error(f"❌ JSON Decode Error for match {match_id}")
        return None


# =============================================================================
# Data Processing Functions
# =============================================================================

def _process_general_section(general_data: dict) -> dict:
    """Process the general section of match data."""
    home_team = general_data.get("homeTeam", {})
    away_team = general_data.get("awayTeam", {})
    
    return {
        "match_id": general_data.get("matchId"),
        "match_name": general_data.get("matchName"),
        "league_id": general_data.get("parentLeagueId"),
        "league_name": general_data.get("leagueName"),
        "match_date_time_UTC": general_data.get("matchTimeUTCDate"),
        "started": general_data.get("started"),
        "finished": general_data.get("finished"),
        "home_team_id": home_team.get("id"),
        "home_team_name": home_team.get("name"),
        "away_team_id": away_team.get("id"),
        "away_team_name": away_team.get("name"),
    }


def _further_process_player_stats(raw_stats_list: list) -> dict:
    """Process raw player stats into simplified format."""
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


def _process_individual_player_stats(player_stats: dict) -> dict:
    """Process individual player statistics."""
    stats = {
        "name": player_stats.get("name"),
        "player_fotmob_id": player_stats.get("id"),
        "player_team_id": player_stats.get("teamId"),
        "player_team_name": player_stats.get("teamName"),
        "isGoalkeeper": player_stats.get("isGoalkeeper"),
    }

    stats_for_processing = player_stats.get("stats")
    simplified_stats = _further_process_player_stats(stats_for_processing)
    stats.update(simplified_stats)
    
    return stats


def _process_content_section(content_data: dict) -> List[dict]:
    """Process content section to extract player stats."""
    if not content_data:
        logger.warning("_process_content_section received empty content_data.")
        return []
    
    if content_data.get("playerStats") is None:
        logger.debug("playerStats is None (match may not have started).")
        return []
    
    player_stats_data = content_data.get("playerStats", {})
    player_stats = []

    for player_id in player_stats_data.keys():
        individual_stats = _process_individual_player_stats(
            player_stats_data.get(player_id, {})
        )
        player_stats.append(individual_stats)

    # Get additional stats (yellow cards, goals, etc.)
    additional_stats = process_additional_stats(content_data.get("matchFacts", {}))
    additional_stats = {str(k): v for k, v in additional_stats.items()}

    # Merge additional stats into player stats
    for player in player_stats:
        player_id = str(player.get("player_fotmob_id"))
        if player_id in additional_stats:
            for new_stats in additional_stats[player_id]:
                player.update(new_stats)
    
    return player_stats


def _process_match_info(response_data: Optional[dict]) -> dict:
    """Process complete match information from API response."""
    if response_data is None:
        logger.warning("_process_match_info received None response_data")
        return {}

    processed_info = _process_general_section(response_data.get("general", {}))

    # Only process player stats if match has started
    if processed_info.get("started"):
        player_stats = _process_content_section(response_data.get("content", {}))
        processed_info["player_stats"] = player_stats

    return processed_info


# =============================================================================
# Main Function
# =============================================================================

def get_match_wise_player_stats(
    x_mas: str,
    match_id: str,
    season_file_path: str,
    save_to_json: bool = True,
    save_to_mongodb: bool = True
) -> dict:
    """
    Fetch and store player stats for a specific match.
    
    Args:
        x_mas: X-MAS authentication token
        match_id: Match ID to fetch
        season_file_path: Path to season file (for context)
        save_to_json: Whether to save to JSON (default: True for debugging)
        save_to_mongodb: Whether to save to MongoDB (default: True)
        
    Returns:
        Processed match stats dictionary
    """
    # Debug: Check if x_mas is being passed
    if not x_mas:
        logger.error(f"❌ X-MAS token is None/empty for match {match_id}!")
        print(f"❌ X-MAS token is None/empty for match {match_id}!")
        return {}
    
    # Fetch data from API
    response_data = _make_request(x_mas, match_id)
    final_stats = _process_match_info(response_data)
    
    if not final_stats:
        logger.warning(f"No stats processed for match {match_id}")
        return {}
    
    # =========================================================================
    # Save to JSON (comment out to skip - reduces overhead)
    # =========================================================================
    if save_to_json:
        save_data_to_json(season_file_path, final_stats, match_id)
    
    # =========================================================================
    # Save to MongoDB
    # =========================================================================
    if save_to_mongodb and MONGODB_AVAILABLE:
        success, stats = _save_match_to_mongodb(final_stats, season_file_path)
        if success:
            logger.debug(f"✅ MongoDB: Match {match_id} saved. "
                        f"Player stats: {stats['player_stats']}")
        else:
            logger.warning(f"⚠️ MongoDB save had issues for match {match_id}")
    
    print(f"Processed match {match_id} from {season_file_path}")
    return final_stats


# =============================================================================
# Batch Processing Function
# =============================================================================

def process_matches_batch(
    x_mas: str,
    match_ids: List[str],
    season_file_path: str,
    save_to_json: bool = True,
    save_to_mongodb: bool = True
) -> Dict[str, int]:
    """
    Process multiple matches in batch.
    
    Args:
        x_mas: X-MAS authentication token
        match_ids: List of match IDs to process
        season_file_path: Path to season file
        save_to_json: Whether to save JSON files
        save_to_mongodb: Whether to save to MongoDB
        
    Returns:
        Dictionary with processing stats
    """
    stats = {
        "total": len(match_ids),
        "processed": 0,
        "failed": 0
    }
    
    for match_id in match_ids:
        try:
            result = get_match_wise_player_stats(
                x_mas=x_mas,
                match_id=match_id,
                season_file_path=season_file_path,
                save_to_json=save_to_json,
                save_to_mongodb=save_to_mongodb
            )
            
            if result:
                stats["processed"] += 1
            else:
                stats["failed"] += 1
                
        except Exception as e:
            logger.error(f"Error processing match {match_id}: {e}")
            stats["failed"] += 1
    
    logger.info(f"Batch complete: {stats['processed']}/{stats['total']} processed, "
                f"{stats['failed']} failed")
    
    return stats