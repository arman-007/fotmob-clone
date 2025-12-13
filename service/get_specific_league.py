"""
Get Specific League Module

Fetches league-specific data including seasons from FotMob API and stores in:
1. JSON files (for debugging/backup)
2. MongoDB (primary storage)

The JSON saving is kept for debugging purposes but can be commented out.
"""

from dotenv import load_dotenv
import json
import logging
import os
import requests

from service.get_auth_headers import capture_x_mas

# MongoDB imports - wrapped in try/except for when running without MongoDB
try:
    from db import get_mongodb_service, extract_season_from_path
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

load_dotenv()

logger = logging.getLogger(__name__)

URL = os.environ.get("URL")


def _write_data_to_json(data: dict, filepath: str) -> None:
    """
    Write data to JSON file.
    
    This function is kept for debugging purposes.
    """
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        logger.debug(f"Saved response to {filepath}")
    except IOError as e:
        logger.error(f"Failed to write JSON to {filepath}: {e}")


def _save_season_to_mongodb(
    league_id: str,
    season_id: str,
    data: dict,
    all_seasons: list
) -> bool:
    """
    Save season data to MongoDB.
    
    Args:
        league_id: League ID
        season_id: Season ID (e.g., "2024/25" or "2024-25")
        data: Raw API response data
        all_seasons: List of all available seasons
        
    Returns:
        bool: Success status
    """
    mongo = get_mongodb_service()
    
    # Normalize season_id (replace / with -)
    normalized_season_id = season_id.replace("/", "-")
    league_season_key = f"{league_id}_{normalized_season_id}"
    
    # Extract season metadata
    details = data.get("details", {})
    fixtures = data.get("fixtures", {})
    all_matches = fixtures.get("allMatches", [])
    
    season_doc = {
        "league_id": str(league_id),
        "season_id": normalized_season_id,
        "league_season_key": league_season_key,
        "league_name": details.get("name", ""),
        "country_code": details.get("ccode", ""),
        "all_available_seasons": all_seasons,
        "stats_summary": {
            "total_matches": len(all_matches),
            "completed_matches": sum(1 for m in all_matches if m.get("status", {}).get("finished", False)),
            "total_goals": 0  # Can be computed later
        }
    }
    
    success, error = mongo.insert_season(season_doc, validate=True)
    
    if not success:
        logger.warning(f"Failed to save season {league_season_key}: {error}")
        return False
    
    # Update league with season info
    try:
        mongo.leagues.update_one(
            {"league_id": str(league_id)},
            {
                "$addToSet": {
                    "seasons": {
                        "season_id": normalized_season_id,
                        "is_current": normalized_season_id == all_seasons[0].replace("/", "-") if all_seasons else False
                    }
                }
            }
        )
    except Exception as e:
        logger.warning(f"Failed to update league seasons: {e}")
    
    logger.debug(f"✅ Saved season {league_season_key} to MongoDB")
    return True


def get_specific_league_data(
    league_id: str,
    x_mas: str = None,
    save_to_json: bool = True,
    save_to_mongodb: bool = True
):
    """
    Fetch league-specific data including all seasons.
    
    Args:
        league_id: League ID to fetch
        x_mas: Optional X-MAS token (will be captured if not provided)
        save_to_json: Whether to save to JSON files (default: True for debugging)
        save_to_mongodb: Whether to save to MongoDB (default: True)
        
    Returns:
        Latest season data or None on failure
    """
    url = f"{URL}/leagues"
    
    # Get fresh X-MAS token if not provided
    if not x_mas:
        logger.info(f"Capturing fresh X-MAS token for league {league_id}...")
        x_mas = capture_x_mas()
        
    if not x_mas:
        logger.error(f"Failed to capture X-MAS token for league {league_id}")
        return None
    
    params = {
        'id': league_id,
    }

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

    try:
        # Fetch initial league data (current season)
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()

        # Get latest 10 seasons
        all_available_seasons = data.get('allAvailableSeasons', [])
        latest_10_seasons = all_available_seasons[:10]
        
        if not latest_10_seasons:
            logger.warning(f"No seasons found for league {league_id}")
            return data

        # =====================================================================
        # Process first (current) season
        # =====================================================================
        current_season = latest_10_seasons[0]
        normalized_season = current_season.replace('/', '-')
        
        # Save to JSON (comment out to skip)
        if save_to_json:
            output_dir = f"output/leagues/{league_id}/{normalized_season}"
            os.makedirs(output_dir, exist_ok=True)
            filepath = f"{output_dir}/league_info_leagueID_{league_id}_season_{normalized_season}.json"
            _write_data_to_json(data, filepath)
        
        # Save to MongoDB
        if save_to_mongodb and MONGODB_AVAILABLE:
            _save_season_to_mongodb(league_id, current_season, data, latest_10_seasons)

        # =====================================================================
        # Process remaining seasons
        # =====================================================================
        for season in latest_10_seasons[1:]:
            params['season'] = season
            
            try:
                response = requests.get(url, params=params, headers=headers, timeout=15)
                response.raise_for_status()
                season_data = response.json()
                
                normalized_season = season.replace('/', '-')
                
                # Save to JSON (comment out to skip)
                if save_to_json:
                    season_output_dir = f"output/leagues/{league_id}/{normalized_season}"
                    os.makedirs(season_output_dir, exist_ok=True)
                    filepath = f"{season_output_dir}/league_info_leagueID_{league_id}_season_{normalized_season}.json"
                    _write_data_to_json(season_data, filepath)
                
                # Save to MongoDB
                if save_to_mongodb and MONGODB_AVAILABLE:
                    _save_season_to_mongodb(league_id, season, season_data, latest_10_seasons)
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch season {season} for league {league_id}: {e}")
                continue
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON for season {season}: {e}")
                continue
        
        logger.info(f"✅ Successfully fetched data for LeagueID {league_id} ({len(latest_10_seasons)} seasons)")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request failed for league {league_id}: {e}")
        return None
    
    except json.JSONDecodeError:
        logger.error(f"❌ Failed to decode JSON for league {league_id}")
        return None


if __name__ == "__main__":
    # Test with a specific league
    mongo = get_mongodb_service()
    mongo.connect()
    
    test_league_id = "47"  # Premier League
    get_specific_league_data(test_league_id, save_to_json=True, save_to_mongodb=True)