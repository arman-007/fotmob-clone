"""
Get Daily Matches Module

Fetches matches for a specific date from FotMob API.
Used by the daily pipeline for incremental updates.

Features:
- Fetch all matches for a given date
- Optional league filtering
- Save to JSON (optional) and return match data

NOTE: All IDs (match_id, league_id) are stored as INTEGERS for consistency.
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional, List, Dict, Any, Union

import requests
from dotenv import load_dotenv

from service.auth_utils import get_auth_headers, generate_x_mas_header
from service.playwright_auth import fetch_json_playwright
from utils.get_timezone import get_local_time_zone

load_dotenv()

logger = logging.getLogger(__name__)

URL = os.environ.get("URL")


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


def fetch_matches_by_date(
    date: str,
    league_ids: List[int] = None,
    save_to_json: bool = True,
    output_dir: str = "output/daily"
) -> Optional[Dict[str, Any]]:
    """
    Fetch all matches for a specific date from FotMob API.
    
    Args:
        date: Date string in format YYYYMMDD (e.g., "20241215")
        league_ids: Optional list of league IDs to filter (as integers, None = all leagues)
        save_to_json: Whether to save raw response to JSON file
        output_dir: Directory for JSON output
        
    Returns:
        Dictionary containing:
        - raw_data: Original API response
        - matches: List of match info dicts with league context
        - match_ids: List of match IDs (as integers)
        - leagues: Dict mapping league_id to league info
        
        Returns None on failure.
    """
    if not URL:
        logger.error("URL environment variable is not set")
        return None
    
    # Get timezone
    timezone = get_local_time_zone()
    
    # Generate all required headers (dynamic x-mas + cookies)
    api_path = f"/api/data/matches?date={date}&timezone={timezone}"
    headers = get_auth_headers(api_path)
    
    url = f"{URL}/matches"
    params = {
        'date': date,
        'timezone': str(timezone),
    }
    
    try:
        logger.info(f"Fetching matches for date: {date}")
        response = requests.get(url, params=params, headers=headers, timeout=15)
        
        if response.status_code == 403:
            logger.warning(f"403 Forbidden for date {date}, falling back to Playwright...")
            fallback_path = f"/api/data/matches?date={date}&timezone={timezone}"
            api_url = f"https://www.fotmob.com{fallback_path}"
            # Pass x-mas header for the in-page fetch
            fallback_headers = {"x-mas": generate_x_mas_header(fallback_path)}
            data = fetch_json_playwright(api_url, headers=fallback_headers)
            if not data:
                return None
        else:
            response.raise_for_status()
            data = response.json()
        
        if not data:
            logger.warning(f"Empty response for date {date}")
            return None
        
        # Save raw response to JSON if requested
        if save_to_json:
            _save_matches_json(data, date, output_dir)
        
        # Process and structure the response
        result = _process_matches_response(data, league_ids)
        result["raw_data"] = data
        result["date"] = date
        
        logger.info(f"✅ Found {len(result['match_ids'])} matches for {date}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error fetching matches for {date}: {e}")
        return None


def _save_matches_json(data: dict, date: str, output_dir: str) -> None:
    """Save matches data to JSON file."""
    try:
        os.makedirs(output_dir, exist_ok=True)
        filepath = f"{output_dir}/matches_{date}.json"
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        
        logger.debug(f"✅ Saved matches to {filepath}")
        
    except IOError as e:
        logger.error(f"❌ Failed to save matches JSON: {e}")


def _process_matches_response(
    data: dict,
    league_ids: List[int] = None
) -> Dict[str, Any]:
    """
    Process raw API response into structured format.
    
    Args:
        data: Raw API response
        league_ids: Optional list of league IDs to filter (as integers)
        
    Returns:
        Structured dict with matches, match_ids, and leagues
    """
    result = {
        "matches": [],
        "match_ids": [],
        "leagues": {},
        "stats": {
            "total_leagues": 0,
            "total_matches": 0,
            "filtered_matches": 0
        }
    }
    
    # Convert league_ids to set for faster lookup
    league_filter = set(league_ids) if league_ids else None
    
    leagues_data = data.get("leagues", [])
    result["stats"]["total_leagues"] = len(leagues_data)
    
    for league in leagues_data:
        league_id = safe_int(league.get("id"))
        if not league_id:
            continue
            
        league_name = league.get("name", "Unknown League")
        league_country = league.get("ccode", "")
        
        # Store league info
        result["leagues"][league_id] = {
            "league_id": league_id,  # int
            "name": league_name,
            "country_code": league_country
        }
        
        matches_list = league.get("matches", [])
        result["stats"]["total_matches"] += len(matches_list)
        
        for match in matches_list:
            match_id = safe_int(match.get("id"))
            
            if not match_id:
                continue
            
            # Apply league filter if specified
            if league_filter and league_id not in league_filter:
                continue
            
            match_info = {
                "match_id": match_id,  # int
                "league_id": league_id,  # int
                "league_name": league_name,
                "home_team": match.get("home", {}),
                "away_team": match.get("away", {}),
                "status": match.get("status", {}),
                "time": match.get("time", ""),
            }
            
            result["matches"].append(match_info)
            result["match_ids"].append(match_id)  # int
    
    result["stats"]["filtered_matches"] = len(result["match_ids"])
    
    return result


def get_match_ids_from_json(
    date: str,
    output_dir: str = "output/daily",
    league_ids: List[int] = None
) -> Optional[List[int]]:
    """
    Load match IDs from a previously saved JSON file.
    
    Useful for resuming processing without re-fetching from API.
    
    Args:
        date: Date string in format YYYYMMDD
        output_dir: Directory where JSON was saved
        league_ids: Optional list of league IDs to filter (as integers)
        
    Returns:
        List of match IDs (as integers) or None if file not found
    """
    filepath = f"{output_dir}/matches_{date}.json"
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        logger.info(f"📂 Loaded matches from {filepath}")
        
        result = _process_matches_response(data, league_ids)
        return result["match_ids"]
        
    except FileNotFoundError:
        logger.warning(f"⚠️ File not found: {filepath}")
        return None
    except json.JSONDecodeError:
        logger.error(f"❌ Invalid JSON in: {filepath}")
        return None


def get_leagues_from_matches(date: str, output_dir: str = "output/daily") -> Dict[int, dict]:
    """
    Get league information from saved matches JSON.
    
    Args:
        date: Date string in format YYYYMMDD
        output_dir: Directory where JSON was saved
        
    Returns:
        Dict mapping league_id (int) to league info
    """
    filepath = f"{output_dir}/matches_{date}.json"
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        result = _process_matches_response(data, None)
        return result["leagues"]
        
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading leagues: {e}")
        return {}