"""
Get Player Stats Module

Fetches match-wise player statistics from FotMob API.

Storage:
- MongoDB (primary storage) - matches and player_stats collections
- JSON files (optional) - for debugging/backup only

Data Flow:
- Receive league_id and season_id directly (no file path dependency)
- Fetch from API → Process in memory
- Save to MongoDB (primary)
- Save to JSON (optional)

This module works with in-memory data flow, eliminating the need
to read from JSON files. The --no-json flag truly means no JSON.
"""

import json
import logging
import os
import requests
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple, Union
from dotenv import load_dotenv

from get_additional_stats import process_additional_stats
from utils.converters import safe_int, safe_float, parse_datetime, determine_season_from_date
from utils.http_client import get_fotmob_headers

# Shared match processing (used by daily pipeline too)
from service.match_stats_processor import (
    fetch_match_details,
    process_match_response,
    save_match_to_mongodb as _shared_save_match_to_mongodb,
)

# MongoDB imports - wrapped in try/except for when running without MongoDB
try:
    from db import get_mongodb_service
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

load_dotenv()

logger = logging.getLogger(__name__)

URL = os.environ.get("URL")


# =============================================================================
# JSON File Operations (optional, for debugging)
# =============================================================================

def _save_to_json(
    data: dict,
    match_id: int,
    league_id: int,
    season_id: str,
    output_base: str = "output/leagues"
) -> None:
    """
    Save player stats to JSON file.
    
    Args:
        data: Processed match data
        match_id: Match ID
        league_id: League ID
        season_id: Season ID
        output_base: Base output directory
    """
    output_dir = f"{output_base}/{league_id}/{season_id}/player_stats"
    filename = f"player_stats_matchID_{match_id}.json"
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, default=str)
        
        logger.debug(f"✅ JSON saved to: {filepath}")
        
    except IOError as e:
        logger.error(f"❌ Failed to write to JSON: {e}")
    except TypeError as e:
        logger.error(f"❌ Serialization error: {e}")




# =============================================================================
# Main Function
# =============================================================================

def get_match_wise_player_stats(
    x_mas: str,
    match_id: Union[int, str],
    league_id: Union[int, str] = None,
    season_id: str = None,
    save_to_json: bool = True,
    save_to_mongodb: bool = True
) -> dict:
    """
    Fetch and store player stats for a specific match.
    
    Uses in-memory data flow - no file reads required.
    
    Args:
        x_mas: X-MAS authentication token
        match_id: Match ID to fetch (int or str)
        league_id: League ID (int or str) - passed directly, no file path needed
        season_id: Season ID (str, e.g., "2024-2025") - passed directly
        save_to_json: Whether to save to JSON (default: True for debugging)
        save_to_mongodb: Whether to save to MongoDB (default: True)
        
    Returns:
        Processed match stats dictionary
    """
    match_id = safe_int(match_id)
    league_id = safe_int(league_id)
    
    if not x_mas:
        logger.error(f"❌ X-MAS token is None/empty for match {match_id}!")
        return {}
    
    if not match_id:
        logger.error(f"❌ Invalid match_id!")
        return {}
    
    # Fetch data from API (uses shared fetch/process from match_stats_processor)
    response_data = fetch_match_details(x_mas, match_id)
    final_stats = process_match_response(response_data)
    
    if not final_stats:
        logger.warning(f"No stats processed for match {match_id}")
        return {}
    
    # Use league_id from response if not provided
    if not league_id:
        league_id = safe_int(final_stats.get("league_id"))
    
    # Determine season from match date if not provided
    if not season_id:
        season_id = determine_season_from_date(final_stats.get("match_date_time_UTC"))
    
    # =========================================================================
    # Save to JSON (optional)
    # =========================================================================
    if save_to_json and league_id and season_id:
        _save_to_json(final_stats, match_id, league_id, season_id)
    
    # =========================================================================
    # Save to MongoDB
    # =========================================================================
    if save_to_mongodb and MONGODB_AVAILABLE:
        success, stats = _shared_save_match_to_mongodb(final_stats, league_id, season_id, safe_update=False)
        if success:
            logger.debug(f"✅ MongoDB: Match {match_id} saved. "
                        f"Player stats: {stats['player_stats']}")
        else:
            logger.warning(f"⚠️ MongoDB save had issues for match {match_id}")
    
    logger.info(f"Processed match {match_id} (league: {league_id}, season: {season_id})")
    return final_stats


# =============================================================================
# Batch Processing Function
# =============================================================================

def process_matches_batch(
    x_mas: str,
    match_ids: List[int],
    league_id: int,
    season_id: str,
    save_to_json: bool = True,
    save_to_mongodb: bool = True
) -> Dict[str, int]:
    """
    Process multiple matches in batch.
    
    Args:
        x_mas: X-MAS authentication token
        match_ids: List of match IDs to process (integers)
        league_id: League ID (int)
        season_id: Season ID (str)
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
                league_id=league_id,
                season_id=season_id,
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