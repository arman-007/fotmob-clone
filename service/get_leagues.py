"""
Get Leagues Module

Fetches all leagues from FotMob API and stores them in:
1. JSON file (for debugging/backup)
2. MongoDB (primary storage)

The JSON saving is kept for debugging purposes but can be commented out
to reduce computational overhead.
"""

import brotli
import logging
import sys
import requests
import json
import os
from dotenv import load_dotenv

from service.get_auth_headers import capture_x_mas

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
    Comment out the call to this function in get_all_leagues() to skip JSON saving.
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
        page_url = league.get("pageUrl", "")
        if page_url and not page_url.startswith("http"):
            page_url = f"{FOTMOB_BASE_URL}{page_url}"
        
        leagues_to_insert.append({
            "league_id": str(league["id"]),
            "name": league["name"],
            "localized_name": league.get("localizedName", league["name"]),
            "country_code": league.get("ccode", ""),
            "page_url": page_url,
            "category": "popular"
        })
    
    # Process international leagues
    for league in leagues_data.get("international", []):
        page_url = league.get("pageUrl", "")
        if page_url and not page_url.startswith("http"):
            page_url = f"{FOTMOB_BASE_URL}{page_url}"
        
        leagues_to_insert.append({
            "league_id": str(league["id"]),
            "name": league["name"],
            "localized_name": league.get("localizedName", league["name"]),
            "country_code": league.get("ccode", "INT"),
            "page_url": page_url,
            "category": "international"
        })
    
    # Process country leagues
    for country in leagues_data.get("countries", []):
        for league in country.get("leagues", []):
            page_url = league.get("pageUrl", "")
            if page_url and not page_url.startswith("http"):
                page_url = f"{FOTMOB_BASE_URL}{page_url}"
            
            leagues_to_insert.append({
                "league_id": str(league["id"]),
                "name": league["name"],
                "localized_name": league.get("localizedName", league["name"]),
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


def get_all_leagues(save_to_json: bool = True, save_to_mongodb: bool = True):
    """
    Fetch and store all leagues from FotMob API.
    
    Args:
        save_to_json: Whether to save to JSON file (default: True for debugging)
        save_to_mongodb: Whether to save to MongoDB (default: True)
        
    Returns:
        Dictionary containing leagues data
    """
    URL = os.environ.get("URL")
    if not URL:
        logger.error("URL environment variable is not set.")
        sys.exit(1)
    
    # Get fresh X-MAS token
    x_mas = capture_x_mas()
    
    url = f"{URL}/allLeagues?locale=en&country=BGD"
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'x-mas': x_mas
    }
    
    try:
        logger.info("Fetching leagues data from FotMob API")
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        content_encoding = response.headers.get('Content-Encoding', '')
        logger.debug(f"Content Encoding: {content_encoding}")

        # Handle response content
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
        
        # Structure the leagues data
        leagues_data = {
            "popular": [],
            "international": [],
            "countries": []
        }
        
        # Extract popular leagues
        leagues_data["popular"] = [
            {
                "id": league["id"],
                "name": league["name"],
                "localizedName": league.get("localizedName", league["name"]),
                "pageUrl": league.get("pageUrl", ""),
                "ccode": league.get("ccode", "")
            }
            for league in data.get("popular", [])
        ]
        
        # Extract international leagues
        leagues_data["international"] = [
            {
                "id": league["id"],
                "name": league["name"],
                "localizedName": league.get("localizedName", league["name"]),
                "pageUrl": league.get("pageUrl", ""),
                "ccode": league.get("ccode", "INT")
            }
            for category in data.get("international", [])
            for league in category.get("leagues", [])
        ]
        
        # Extract country leagues
        leagues_data["countries"] = [
            {
                "country": country["name"],
                "ccode": country.get("ccode", ""),
                "leagues": [
                    {
                        "id": league["id"],
                        "name": league["name"],
                        "localizedName": league.get("localizedName", league["name"]),
                        "pageUrl": league.get("pageUrl", ""),
                        "ccode": league.get("ccode", country.get("ccode", ""))
                    }
                    for league in country.get("leagues", [])
                ]
            }
            for country in data.get("countries", [])
        ]

        # =====================================================================
        # Save to JSON (comment out to skip - reduces overhead)
        # =====================================================================
        if save_to_json:
            _save_leagues_to_json(leagues_data)
        
        # =====================================================================
        # Save to MongoDB
        # =====================================================================
        if save_to_mongodb and MONGODB_AVAILABLE:
            _save_leagues_to_mongodb(leagues_data)
        
        logger.info(f"Successfully processed {len(leagues_data['popular'])} popular leagues, "
                    f"{len(leagues_data['international'])} international leagues, "
                    f"and {len(leagues_data['countries'])} countries with leagues.")

        return leagues_data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from FotMob API: {e}")
        sys.exit(1)


if __name__ == "__main__":
    logger.info("Program Started")
    logger.info(f"{'='*30} Get all leagues {'='*30}")
    
    # Initialize MongoDB connection and create indexes
    mongo = get_mongodb_service()
    mongo.connect()
    mongo.create_indexes()
    
    get_all_leagues(save_to_json=True, save_to_mongodb=True)