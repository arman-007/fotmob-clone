"""
Get Leagues Module

Fetches all leagues from FotMob API.

Storage:
- MongoDB (primary storage) - always saves for persistence
- JSON files (optional) - for debugging/backup only

Data Flow:
- Fetch from API → Keep in memory
- Save to MongoDB (for persistence)
- Save to JSON (optional, controlled by save_to_json flag)
- Return in-memory data (no file reads needed)

This allows --no-json to truly mean NO JSON files.
"""

import logging
import sys
import requests
import json
import os
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

from service.get_auth_headers import capture_x_mas
from utils.converters import safe_int
from utils.http_client import get_fotmob_headers

# MongoDB imports - wrapped in try/except for when running without MongoDB
try:
    from db import get_mongodb_service, validate_league
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

load_dotenv()

logger = logging.getLogger(__name__)


def _save_leagues_to_json(leagues_data: dict, output_dir: str = 'output') -> None:
    """
    Save leagues data to JSON file.
    
    This function is kept for debugging purposes.
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        filepath = f'{output_dir}/leagues_data.json'
        
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(leagues_data, file, indent=4)
        
        logger.info(f"✅ Leagues data saved to JSON: {filepath}")
        
    except IOError as e:
        logger.error(f"❌ Failed to save leagues to JSON: {e}")


def _save_leagues_to_mongodb(leagues_data: dict) -> dict:
    """
    Save leagues data to MongoDB.
    
    Args:
        leagues_data: Dictionary containing popular, international, and countries leagues
        
    Returns:
        Dict with insertion stats
    """
    if not MONGODB_AVAILABLE:
        logger.warning("MongoDB not available, skipping save")
        return {"total": 0, "success": 0, "errors": 0}
    
    mongo = get_mongodb_service()
    
    stats = {"total": 0, "success": 0, "errors": 0}
    leagues_to_insert = []
    
    # Base URL for FotMob
    FOTMOB_BASE_URL = "https://www.fotmob.com"
    
    # Process popular leagues
    for league in leagues_data.get("popular", []):
        league_id = safe_int(league.get("id"))
        if not league_id:
            continue
            
        page_url = league.get("pageUrl", "")
        if page_url and not page_url.startswith("http"):
            page_url = f"{FOTMOB_BASE_URL}{page_url}"
        
        leagues_to_insert.append({
            "league_id": league_id,  # int
            "name": league.get("name", ""),
            "localized_name": league.get("localizedName", league.get("name", "")),
            "country_code": league.get("ccode", ""),
            "page_url": page_url,
            "category": "popular"
        })
    
    # Process international leagues
    for league in leagues_data.get("international", []):
        league_id = safe_int(league.get("id"))
        if not league_id:
            continue
            
        page_url = league.get("pageUrl", "")
        if page_url and not page_url.startswith("http"):
            page_url = f"{FOTMOB_BASE_URL}{page_url}"
        
        leagues_to_insert.append({
            "league_id": league_id,  # int
            "name": league.get("name", ""),
            "localized_name": league.get("localizedName", league.get("name", "")),
            "country_code": league.get("ccode", "INT"),
            "page_url": page_url,
            "category": "international"
        })
    
    # Process country leagues
    for country in leagues_data.get("countries", []):
        for league in country.get("leagues", []):
            league_id = safe_int(league.get("id"))
            if not league_id:
                continue
                
            page_url = league.get("pageUrl", "")
            if page_url and not page_url.startswith("http"):
                page_url = f"{FOTMOB_BASE_URL}{page_url}"
            
            leagues_to_insert.append({
                "league_id": league_id,  # int
                "name": league.get("name", ""),
                "localized_name": league.get("localizedName", league.get("name", "")),
                "country_code": league.get("ccode", country.get("ccode", "")),
                "page_url": page_url,
                "category": "domestic"
            })
    
    # Bulk insert with validation
    stats["total"] = len(leagues_to_insert)
    result = mongo.insert_leagues_bulk(leagues_to_insert, validate=True)
    
    stats["success"] = result.get("inserted", 0) + result.get("modified", 0)
    stats["errors"] = result.get("errors", 0)
    
    logger.info(f"✅ MongoDB: Processed {stats['total']} leagues. "
                f"Success: {stats['success']}, Errors: {stats['errors']}")
    
    return stats


def get_all_leagues(
    x_mas: str = None,
    save_to_json: bool = True,
    save_to_mongodb: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Fetch and store all leagues from FotMob API.
    
    Returns data in-memory for immediate use by the pipeline.
    No file reads required - the returned dict contains everything needed.
    
    Args:
        save_to_json: Whether to save to JSON file (default: True for debugging)
        save_to_mongodb: Whether to save to MongoDB (default: True)
        
    Returns:
        Dictionary containing:
        - popular: List of popular leagues with integer IDs
        - international: List of international leagues with integer IDs
        - countries: List of countries with nested leagues (integer IDs)
        
        Returns None on failure.
    """
    URL = os.environ.get("URL")
    if not URL:
        logger.error("URL environment variable is not set.")
        return None
    
    # Get X-MAS token (use provided or capture fresh)
    if not x_mas:
        x_mas = capture_x_mas()

    if not x_mas:
        logger.error("Failed to capture X-MAS token")
        return None

    country_code = os.environ.get("CCODE3", "BGD")
    url = f"{URL}/allLeagues?locale=en&country={country_code}"
    headers = get_fotmob_headers(x_mas)
    
    try:
        logger.info("Fetching leagues data from FotMob API")
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        # Handle response content
        content_encoding = response.headers.get('Content-Encoding', '')
        if content_encoding == 'br':
            response_content = response.content
        else:
            response_content = response.text

        # Parse JSON
        try:
            if isinstance(response_content, bytes):
                data = json.loads(response_content.decode('utf-8'))
            else:
                data = json.loads(response_content)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
            return None

        if not data:
            logger.error("Received empty response from the API.")
            return None
        
        # Structure the leagues data with integer IDs
        leagues_data = {
            "popular": [],
            "international": [],
            "countries": []
        }
        
        # Extract popular leagues
        for league in data.get("popular", []):
            league_id = safe_int(league.get("id"))
            if league_id:
                leagues_data["popular"].append({
                    "id": league_id,  # int
                    "name": league.get("name", ""),
                    "localizedName": league.get("localizedName", league.get("name", "")),
                    "pageUrl": league.get("pageUrl", ""),
                    "ccode": league.get("ccode", "")
                })
        
        # Extract international leagues
        for category in data.get("international", []):
            for league in category.get("leagues", []):
                league_id = safe_int(league.get("id"))
                if league_id:
                    leagues_data["international"].append({
                        "id": league_id,  # int
                        "name": league.get("name", ""),
                        "localizedName": league.get("localizedName", league.get("name", "")),
                        "pageUrl": league.get("pageUrl", ""),
                        "ccode": league.get("ccode", "INT")
                    })
        
        # Extract country leagues
        for country in data.get("countries", []):
            country_leagues = []
            for league in country.get("leagues", []):
                league_id = safe_int(league.get("id"))
                if league_id:
                    country_leagues.append({
                        "id": league_id,  # int
                        "name": league.get("name", ""),
                        "localizedName": league.get("localizedName", league.get("name", "")),
                        "pageUrl": league.get("pageUrl", ""),
                        "ccode": league.get("ccode", country.get("ccode", ""))
                    })
            
            if country_leagues:
                leagues_data["countries"].append({
                    "country": country.get("name", ""),
                    "ccode": country.get("ccode", ""),
                    "leagues": country_leagues
                })

        # =====================================================================
        # Save to JSON (optional)
        # =====================================================================
        if save_to_json:
            _save_leagues_to_json(leagues_data)
        
        # =====================================================================
        # Save to MongoDB
        # =====================================================================
        if save_to_mongodb and MONGODB_AVAILABLE:
            _save_leagues_to_mongodb(leagues_data)
        
        # Count totals
        popular_count = len(leagues_data['popular'])
        international_count = len(leagues_data['international'])
        country_leagues_count = sum(len(c['leagues']) for c in leagues_data['countries'])
        
        logger.info(f"Successfully processed {popular_count} popular leagues, "
                    f"{international_count} international leagues, "
                    f"and {country_leagues_count} country leagues from {len(leagues_data['countries'])} countries.")

        return leagues_data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from FotMob API: {e}")
        return None


if __name__ == "__main__":
    logger.info("Program Started")
    logger.info(f"{'='*30} Get all leagues {'='*30}")
    
    # Initialize MongoDB connection and create indexes
    if MONGODB_AVAILABLE:
        mongo = get_mongodb_service()
        mongo.connect()
        mongo.create_indexes()
    
    result = get_all_leagues(save_to_json=True, save_to_mongodb=True)
    
    if result:
        print(f"\nPopular leagues: {len(result['popular'])}")
        print(f"International leagues: {len(result['international'])}")
        print(f"Countries: {len(result['countries'])}")
        
        # Show sample
        print("\nSample popular leagues:")
        for league in result['popular'][:5]:
            print(f"  - {league['id']}: {league['name']}")