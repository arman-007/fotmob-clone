#!/usr/bin/env python3
"""
Football Stats Pipeline (Historical Data Ingestion)

Main orchestration script for fetching and storing historical football statistics.
Processes leagues, seasons, and matches from FotMob API into MongoDB.

Storage Options:
    - JSON files (for debugging/backup) - can be disabled via --no-json
    - MongoDB (primary storage) - recommended for production

Features:
    - Checkpoint/resume: Automatically resumes from where it stopped
    - Failed match retry: Retries only failed matches on subsequent runs
    - Progress tracking: Tracks processing status in MongoDB
    - Flexible league selection: Process popular, international, or country leagues
    - Skip list: Exclude specific leagues from processing

Usage:
    python pipeline.py                              # Default: process ALL leagues
    python pipeline.py --source popular             # Process popular leagues only
    python pipeline.py --source countries           # Process all country leagues
    python pipeline.py --source international       # Process international leagues
    python pipeline.py --no-json                    # Skip JSON files (faster)
    python pipeline.py --no-mongodb                 # Skip MongoDB (debugging only)
    python pipeline.py --build-players              # Build player profiles after ingestion
    python pipeline.py --force                      # Force re-process even completed seasons
    python pipeline.py --retry-failed               # Only retry failed matches
    python pipeline.py --status                     # Show pipeline progress status
    python pipeline.py --league-limit 5             # Process only first 5 leagues (testing)
    python pipeline.py --skip-leagues 47,55         # Skip specific league IDs

CLI Flags:
    --source SOURCE         League source to process:
                            - 'popular': Top leagues (default)
                            - 'international': International competitions
                            - 'countries': All domestic leagues by country
                            - 'all': All available leagues
    --no-json               Skip saving JSON files (faster, less disk usage)
    --no-mongodb            Skip saving to MongoDB (debugging mode)
    --build-players         Build aggregated player profiles after ingestion
    --league-limit N        Limit number of leagues to process (for testing)
    --skip-leagues IDS      Comma-separated league IDs to skip (e.g., "47,55,87")
    --force                 Force re-process all seasons (ignore completed status)
    --retry-failed          Only retry previously failed matches
    --status                Show pipeline progress status and exit
    -d, --date DATE         Date parameter (default: today, format: YYYYMMDD)
    -v, --verbose           Enable verbose/debug logging

Examples:
    # Process ALL leagues (default)
    python pipeline.py

    # Process only popular leagues
    python pipeline.py --source popular

    # Process all country leagues, skip JSON for speed
    python pipeline.py --source countries --no-json

    # Process international tournaments
    python pipeline.py --source international

    # Test with 2 leagues from all sources
    python pipeline.py --league-limit 2

    # Resume from where it stopped (automatic)
    python pipeline.py

    # Force re-process everything
    python pipeline.py --force

    # Only retry failed matches
    python pipeline.py --retry-failed

    # Skip problematic leagues
    python pipeline.py --skip-leagues 10913,285,9173

    # Check progress
    python pipeline.py --status
"""

import argparse
import logging
import time
import os
import sys
from datetime import datetime
from datetime import timezone
from typing import List, Optional, Set

# Service imports
from service.get_auth_headers import capture_auth_info
from service.auth_utils import set_auth_info
from service.get_leagues import get_all_leagues
from service.get_specific_league import get_specific_league_data
from service.get_player_stats import get_match_wise_player_stats

# MongoDB imports - wrapped in try/except for when running without MongoDB
try:
    from db import get_mongodb_service, MongoDBConfig, get_pipeline_state_manager
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    get_mongodb_service = None
    MongoDBConfig = None
    get_pipeline_state_manager = None


# =============================================================================
# Logging Configuration
# =============================================================================

def setup_logging(verbose: bool = False):
    """Configure logging for the pipeline."""
    os.makedirs('logs', exist_ok=True)
    
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        handlers=[
            logging.FileHandler('logs/pipeline_log.txt', mode='a'),
            logging.StreamHandler(sys.stdout)
        ],
        format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s',
    )
    
    return logging.getLogger(__name__)


logger = logging.getLogger(__name__)


# =============================================================================
# Helper Functions
# =============================================================================

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


def extract_league_ids(league_data: dict, source: str) -> List[int]:
    """
    Extract league IDs from league data based on source type.
    
    Args:
        league_data: Dictionary containing popular, international, and countries leagues
        source: One of 'popular', 'international', 'countries', or 'all'
        
    Returns:
        List of league IDs (as integers)
    """
    league_ids = []
    
    if source in ('popular', 'all'):
        popular = league_data.get("popular", [])
        for league in popular:
            lid = safe_int(league.get("id"))
            if lid:
                league_ids.append(lid)
        logger.info(f"Found {len(popular)} popular leagues")
    
    if source in ('international', 'all'):
        international = league_data.get("international", [])
        for league in international:
            lid = safe_int(league.get("id"))
            if lid:
                league_ids.append(lid)
        logger.info(f"Found {len(international)} international leagues")
    
    if source in ('countries', 'all'):
        countries = league_data.get("countries", [])
        country_count = 0
        for country in countries:
            for league in country.get("leagues", []):
                lid = safe_int(league.get("id"))
                if lid:
                    league_ids.append(lid)
                    country_count += 1
        logger.info(f"Found {country_count} country leagues from {len(countries)} countries")
    
    # Remove duplicates while preserving order
    seen = set()
    unique_ids = []
    for lid in league_ids:
        if lid not in seen:
            seen.add(lid)
            unique_ids.append(lid)
    
    return unique_ids


def parse_skip_leagues(skip_leagues_str: str) -> Set[int]:
    """Parse comma-separated league IDs to skip."""
    if not skip_leagues_str:
        return set()
    
    skip_set = set()
    for lid in skip_leagues_str.split(","):
        lid_int = safe_int(lid.strip())
        if lid_int:
            skip_set.add(lid_int)
    
    return skip_set


# =============================================================================
# MongoDB Initialization
# =============================================================================

def initialize_mongodb(config=None) -> bool:
    """
    Initialize MongoDB connection and create indexes.
    
    Args:
        config: Optional MongoDB configuration
        
    Returns:
        bool: True if successful
    """
    if not MONGODB_AVAILABLE:
        logger.warning("MongoDB module not available. Skipping MongoDB initialization.")
        return False
    
    try:
        mongo = get_mongodb_service(config)
        mongo.connect()
        mongo.create_indexes()
        
        logger.info("✅ MongoDB initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize MongoDB: {e}")
        return False


def build_player_profiles():
    """
    Build aggregated player profiles from player_stats collection.
    
    This should be run after all match data has been ingested.
    It creates/updates the 'players' collection with aggregated career stats.
    """
    if not MONGODB_AVAILABLE:
        logger.warning("MongoDB not available, cannot build player profiles")
        return 0
    
    logger.info("Building player profiles...")
    
    mongo = get_mongodb_service()
    
    pipeline = [
        {
            "$group": {
                "_id": "$player_id",
                "name": {"$first": "$name"},
                "current_team_id": {"$last": "$team_id"},
                "current_team_name": {"$last": "$team_name"},
                "is_goalkeeper": {"$first": "$is_goalkeeper"},
                "total_matches": {"$sum": 1},
                "total_goals": {"$sum": "$goals"},
                "total_assists": {"$sum": "$assists"},
                "total_minutes": {"$sum": "$minutes_played"},
                "avg_rating": {"$avg": "$rating"},
                "seasons_data": {
                    "$push": {
                        "league_id": "$league_id",
                        "season_id": "$season_id",
                        "league_season_key": "$league_season_key",
                        "team_id": "$team_id",
                        "team_name": "$team_name",
                        "goals": "$goals",
                        "assists": "$assists",
                        "rating": "$rating",
                        "minutes_played": "$minutes_played"
                    }
                }
            }
        }
    ]
    
    now = datetime.now(timezone.utc)
    players_processed = 0
    
    for player_agg in mongo.player_stats.aggregate(pipeline, allowDiskUse=True):
        player_id = player_agg["_id"]
        
        # Aggregate by season
        seasons_summary = {}
        for entry in player_agg.get("seasons_data", []):
            key = entry.get("league_season_key", "")
            if not key:
                continue
                
            if key not in seasons_summary:
                seasons_summary[key] = {
                    "league_id": entry.get("league_id"),
                    "season_id": entry.get("season_id", ""),
                    "league_season_key": key,
                    "team_id": entry.get("team_id"),
                    "team_name": entry.get("team_name", ""),
                    "matches": 0,
                    "goals": 0,
                    "assists": 0,
                    "minutes_played": 0,
                    "ratings": []
                }
            
            seasons_summary[key]["matches"] += 1
            seasons_summary[key]["goals"] += entry.get("goals", 0) or 0
            seasons_summary[key]["assists"] += entry.get("assists", 0) or 0
            seasons_summary[key]["minutes_played"] += entry.get("minutes_played", 0) or 0
            
            if entry.get("rating"):
                seasons_summary[key]["ratings"].append(entry["rating"])
        
        # Calculate avg rating per season
        seasons_list = []
        for key, s in seasons_summary.items():
            avg_rating = sum(s["ratings"]) / len(s["ratings"]) if s["ratings"] else None
            seasons_list.append({
                "league_id": s["league_id"],
                "season_id": s["season_id"],
                "league_season_key": key,
                "team_id": s["team_id"],
                "team_name": s["team_name"],
                "matches": s["matches"],
                "goals": s["goals"],
                "assists": s["assists"],
                "minutes_played": s["minutes_played"],
                "avg_rating": round(avg_rating, 2) if avg_rating else None
            })
        
        player_doc = {
            "player_id": player_id,
            "name": player_agg.get("name", ""),
            "current_team_id": player_agg.get("current_team_id"),
            "current_team_name": player_agg.get("current_team_name"),
            "is_goalkeeper": player_agg.get("is_goalkeeper", False),
            "total_matches": player_agg.get("total_matches", 0),
            "total_goals": player_agg.get("total_goals", 0),
            "total_assists": player_agg.get("total_assists", 0),
            "total_minutes": player_agg.get("total_minutes", 0),
            "avg_rating": round(player_agg.get("avg_rating", 0), 2) if player_agg.get("avg_rating") else None,
            "seasons": seasons_list,
            "created_at": now,
            "updated_at": now
        }
        
        mongo.players.update_one(
            {"player_id": player_id},
            {"$set": player_doc},
            upsert=True
        )
        
        players_processed += 1
        
        if players_processed % 1000 == 0:
            logger.info(f"Processed {players_processed} players...")
    
    logger.info(f"✅ Built profiles for {players_processed} players.")
    return players_processed


def print_database_stats():
    """Print current database statistics."""
    if not MONGODB_AVAILABLE:
        logger.warning("MongoDB not available, cannot print stats")
        return
    
    mongo = get_mongodb_service()
    stats = mongo.get_collection_stats()
    
    print("\n" + "=" * 50)
    print("DATABASE STATISTICS")
    print("=" * 50)
    for collection, count in stats.items():
        print(f"  {collection}: {count:,} documents")
    print("=" * 50 + "\n")


def show_pipeline_status():
    """Display pipeline progress status."""
    if not MONGODB_AVAILABLE:
        print("MongoDB not available, cannot show status")
        return
    
    state_manager = get_pipeline_state_manager()
    summary = state_manager.get_progress_summary()
    
    print("\n" + "=" * 60)
    print("PIPELINE PROGRESS STATUS")
    print("=" * 60)
    
    print(f"\n📊 Overall Progress:")
    print(f"   Total Seasons Tracked: {summary.get('total_seasons', 0)}")
    print(f"   Total Matches: {summary.get('total_matches', 0):,}")
    print(f"   Processed Matches: {summary.get('processed_matches', 0):,}")
    print(f"   Failed Matches: {summary.get('failed_matches', 0)}")
    
    print(f"\n📋 By Status:")
    for status, count in summary.get('by_status', {}).items():
        emoji = {
            'completed': '✅',
            'in_progress': '🔄',
            'partially_completed': '⚠️',
            'failed': '❌',
            'pending': '⏳'
        }.get(status, '❓')
        print(f"   {emoji} {status}: {count} seasons")
    
    # Show failed matches if any
    failed_matches = state_manager.get_failed_matches()
    if failed_matches:
        print(f"\n❌ Failed Matches (first 10):")
        for fm in failed_matches[:10]:
            print(f"   - League {fm['league_id']}, Season {fm['season_id']}, Match {fm['match_id']}")
            if fm.get('error'):
                print(f"     Error: {fm['error'][:50]}...")
        if len(failed_matches) > 10:
            print(f"   ... and {len(failed_matches) - 10} more")
    
    print("\n" + "=" * 60 + "\n")


# =============================================================================
# Main Pipeline
# =============================================================================

def run_pipeline(
    source: str = "all",
    save_to_json: bool = True,
    save_to_mongodb: bool = True,
    build_players: bool = False,
    league_limit: int = None,
    skip_leagues: Set[int] = None,
    force: bool = False,
    retry_failed_only: bool = False,
    no_browser: bool = False,
    skip_individual_player_stats: bool = False
):
    """
    Run the complete data pipeline with checkpoint/resume support.

    Args:
        source: League source ('popular', 'international', 'countries', 'all')
                Default is 'all' to process all available leagues.
        save_to_json: Whether to save JSON files (for debugging)
        save_to_mongodb: Whether to save to MongoDB (primary storage)
        build_players: Whether to build player profiles after ingestion
        league_limit: Limit number of leagues to process (for testing)
        skip_leagues: Set of league IDs to skip
        force: Force re-process even completed seasons
        retry_failed_only: Only retry failed matches, skip successful ones
    """
    start_time = time.time()
    skip_leagues = skip_leagues or set()
    
    # =========================================================================
    # Step 0: Initialize MongoDB and State Manager (if enabled)
    # =========================================================================
    state_manager = None
    if save_to_mongodb:
        if not initialize_mongodb():
            logger.error("MongoDB initialization failed. Continuing with JSON only.")
            save_to_mongodb = False
        else:
            state_manager = get_pipeline_state_manager()
            logger.info("✅ Pipeline state manager initialized")
    
    # =========================================================================
    # Step 1: Fetch all leagues
    # =========================================================================
    logger.info(f"{'='*30} Step 1: Get all leagues {'='*30}")
    
    # Auth: browser-based capture is optional, dynamic headers always work
    if not no_browser:
        logger.info("Attempting browser-based auth capture...")
        auth_info = capture_auth_info()
        if auth_info:
            set_auth_info(auth_info)
            logger.info("Browser auth captured successfully")
        else:
            logger.warning("Browser auth failed, using dynamic-only x-mas headers")
    else:
        logger.info("No-browser mode: using dynamically generated x-mas headers only")

    # Ensure client_version is loaded (pure HTTP, no browser)
    from service.auth_utils import get_live_client_version, _SESSION_AUTH
    if _SESSION_AUTH["client_version"] is None:
        _SESSION_AUTH["client_version"] = get_live_client_version()
    
    league_data = get_all_leagues(
        save_to_json=save_to_json,
        save_to_mongodb=save_to_mongodb
    )
    
    if not league_data:
        logger.error("Failed to fetch league data. Exiting.")
        return
    
    # Extract league IDs based on source
    league_ids = extract_league_ids(league_data, source)
    
    logger.info(f"Total leagues from '{source}' source: {len(league_ids)}")
    
    # Apply skip list
    if skip_leagues:
        original_count = len(league_ids)
        league_ids = [lid for lid in league_ids if lid not in skip_leagues]
        skipped_count = original_count - len(league_ids)
        logger.info(f"Skipping {skipped_count} leagues from skip list")
    
    # Apply league limit if specified
    if league_limit:
        league_ids = league_ids[:league_limit]
        logger.info(f"Limited to {league_limit} leagues for processing")
    
    logger.info(f"Final count: {len(league_ids)} leagues to process")
    
    # =========================================================================
    # Step 2: Process each league
    # =========================================================================
    total_matches_processed = 0
    total_matches_skipped = 0
    total_matches_failed = 0
    
    for league_idx, league_id in enumerate(league_ids, 1):
        logger.info(f"{'='*30} Processing League {league_idx}/{len(league_ids)}: {league_id} {'='*30}")
        
        # Fetch league/season data - returns in-memory data with match IDs
        league_data_result = get_specific_league_data(
            league_id,  # int
            save_to_json=save_to_json,
            save_to_mongodb=save_to_mongodb
        )
        
        if not league_data_result or not league_data_result.get("seasons"):
            logger.warning(f"Failed to fetch season data for league {league_id}")
            continue
        
        seasons = league_data_result["seasons"]
        logger.info(f"Found {len(seasons)} seasons for league {league_id}")
        
        # =====================================================================
        # Step 3: Process each season (using in-memory data)
        # =====================================================================
        for season_id, season_info in seasons.items():
            # Get match IDs directly from in-memory data (no file read!)
            all_match_ids = season_info.get("match_ids", [])
            
            if not all_match_ids:
                logger.warning(f"No match IDs found for {league_id}_{season_id}")
                continue
            
            logger.info(f"Processing season: {league_id}_{season_id} ({len(all_match_ids)} matches)")
            
            # =================================================================
            # Check if season should be processed (checkpoint/resume)
            # =================================================================
            matches_to_retry = []
            if state_manager and save_to_mongodb:
                should_process, matches_to_retry = state_manager.should_process_season(
                    league_id, season_id, force=force
                )
                
                if not should_process:
                    logger.info(f"⏭️ Skipping season {league_id}_{season_id} (already completed)")
                    continue
                
                if matches_to_retry:
                    logger.info(f"🔄 Retrying {len(matches_to_retry)} failed matches for {league_id}_{season_id}")
            
            # Refresh auth for each season (browser mode only)
            if not no_browser:
                logger.info("Refreshing auth for this season...")
                auth_info = capture_auth_info()
                if auth_info:
                    set_auth_info(auth_info)
                    logger.info("Auth refreshed successfully")
                else:
                    logger.warning("Auth refresh failed, continuing with dynamic headers")
            
            # Determine which matches to process
            if matches_to_retry:
                # Only retry failed matches (matches_to_retry contains int IDs)
                match_ids = [mid for mid in all_match_ids if mid in matches_to_retry]
                logger.info(f"Processing {len(match_ids)} failed matches (out of {len(all_match_ids)} total)")
            elif retry_failed_only and state_manager:
                # Skip already processed matches
                match_ids = [
                    mid for mid in all_match_ids 
                    if not state_manager.is_match_processed(league_id, season_id, mid)
                ]
                logger.info(f"Processing {len(match_ids)} unprocessed matches (out of {len(all_match_ids)} total)")
            else:
                match_ids = all_match_ids
            
            if not match_ids:
                logger.info(f"No matches to process for {league_id}_{season_id}")
                if state_manager:
                    state_manager.mark_season_completed(league_id, season_id)
                continue
            
            logger.info(f"Found {len(match_ids)} matches to process")
            
            # Mark season as in progress
            if state_manager:
                state_manager.mark_season_in_progress(league_id, season_id, len(all_match_ids))
            
            # =================================================================
            # Step 4: Process each match
            # =================================================================
            season_processed = 0
            season_failed = 0
            
            for match_idx, match_id in enumerate(match_ids, 1):
                # match_id is already int from in-memory data
                
                # Skip if already processed (unless force)
                if state_manager and not force and not matches_to_retry:
                    if state_manager.is_match_processed(league_id, season_id, match_id):
                        total_matches_skipped += 1
                        continue
                
                try:
                    result = get_match_wise_player_stats(
                        match_id=match_id,  # int
                        league_id=league_id,  # int - pass directly
                        season_id=season_id,  # str
                        save_to_json=save_to_json,
                        save_to_mongodb=save_to_mongodb,
                        no_browser=no_browser,
                        skip_individual_player_stats=skip_individual_player_stats
                    )
                    
                    if result:
                        total_matches_processed += 1
                        season_processed += 1
                        
                        # Record success
                        if state_manager:
                            state_manager.record_match_processed(league_id, season_id, match_id)
                    else:
                        total_matches_failed += 1
                        season_failed += 1
                        
                        # Record failure
                        if state_manager:
                            state_manager.record_match_failed(
                                league_id, season_id, match_id, 
                                "Empty result from get_match_wise_player_stats"
                            )
                    
                    # Progress logging every 50 matches
                    if total_matches_processed % 50 == 0:
                        logger.info(f"Progress: {total_matches_processed} matches processed, "
                                f"{total_matches_skipped} skipped, {total_matches_failed} failed")
                        
                except Exception as e:
                    logger.error(f"Error processing match {match_id}: {e}")
                    total_matches_failed += 1
                    season_failed += 1
                    
                    # Record failure
                    if state_manager:
                        state_manager.record_match_failed(league_id, season_id, match_id, str(e))
                    continue
            
            # Mark season as completed
            if state_manager:
                state_manager.mark_season_completed(league_id, season_id)
            
            logger.info(f"Season {league_id}_{season_id}: Processed {season_processed}, Failed {season_failed}")
    
    # =========================================================================
    # Step 5: Build player profiles (optional)
    # =========================================================================
    if build_players and save_to_mongodb:
        logger.info(f"{'='*30} Step 5: Building player profiles {'='*30}")
        build_player_profiles()
    
    # =========================================================================
    # Summary
    # =========================================================================
    end_time = time.time()
    duration = end_time - start_time
    
    logger.info(f"{'='*50}")
    logger.info(f"PIPELINE COMPLETE")
    logger.info(f"{'='*50}")
    logger.info(f"Source: {source}")
    logger.info(f"Total leagues processed: {len(league_ids)}")
    logger.info(f"Total matches processed: {total_matches_processed}")
    logger.info(f"Total matches skipped (already done): {total_matches_skipped}")
    logger.info(f"Total matches failed: {total_matches_failed}")
    logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    logger.info(f"{'='*50}")
    
    # Print database stats if MongoDB was used
    if save_to_mongodb:
        print_database_stats()
        
        # Show pipeline status
        if state_manager:
            show_pipeline_status()


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Football Stats Pipeline - Fetch and store historical football statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python pipeline.py                              # Default: process ALL leagues
    python pipeline.py --source popular             # Process only popular leagues
    python pipeline.py --source countries           # Process all country leagues
    python pipeline.py --source international       # Process international leagues
    python pipeline.py --no-json                    # Skip JSON files (faster)
    python pipeline.py --status                     # Show pipeline progress status
    python pipeline.py --force                      # Force re-process all
    python pipeline.py --retry-failed               # Only retry failed matches
    python pipeline.py --league-limit 2             # Test with 2 leagues
    python pipeline.py --skip-leagues 47,55,87      # Skip specific leagues
    python pipeline.py --build-players              # Build player profiles after
        """
    )
    
    # Source selection
    parser.add_argument(
        "--source",
        type=str,
        choices=["popular", "international", "countries", "all"],
        default="all",
        help="League source to process (default: all)"
    )
    
    # Output options
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Skip saving JSON files (faster, less disk usage)"
    )
    
    parser.add_argument(
        "--no-mongodb",
        action="store_true",
        help="Skip saving to MongoDB (debugging mode)"
    )
    
    parser.add_argument(
        "--build-players",
        action="store_true",
        help="Build aggregated player profiles after ingestion"
    )
    
    # League filtering
    parser.add_argument(
        "--league-limit",
        type=int,
        default=None,
        help="Limit number of leagues to process (for testing)"
    )
    
    parser.add_argument(
        "--skip-leagues",
        type=str,
        default=None,
        help="Comma-separated league IDs to skip (e.g., '47,55,87')"
    )
    
    # Processing modes
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-process all seasons (ignore completed status)"
    )
    
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Only retry previously failed matches"
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show pipeline progress status and exit"
    )
    
    parser.add_argument(
        "-d", "--date",
        type=str,
        default=datetime.now().strftime("%Y%m%d"),
        help="Date parameter (default: today)"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose/debug logging"
    )

    # Browser mode
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Never launch a browser. Use only HTTP requests with dynamic auth headers."
    )

    parser.add_argument(
        "--skip-individual-player-stats",
        action="store_true",
        help="Skip populating the player_stats collection in MongoDB. Match documents (with embedded player data) are still saved."
    )

    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser with visible window (for desktop debugging). Ignored if --no-browser is set."
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Configure headless mode for Playwright
    if not args.no_browser:
        try:
            from service.playwright_auth import set_headless_mode
            set_headless_mode(not args.no_headless)
        except ImportError:
            logger.info("Playwright not installed, browser features unavailable")
    
    # If --status flag, just show status and exit
    if args.status:
        if not MONGODB_AVAILABLE:
            print("MongoDB not available. Cannot show status.")
            sys.exit(1)
        
        # Initialize MongoDB to show status
        initialize_mongodb()
        show_pipeline_status()
        print_database_stats()
        sys.exit(0)
    
    # Parse skip leagues
    skip_leagues = parse_skip_leagues(args.skip_leagues)
    
    logger.info("=" * 60)
    logger.info("FOOTBALL STATS PIPELINE STARTED")
    logger.info("=" * 60)
    logger.info(f"Configuration:")
    logger.info(f"  - Source: {args.source}")
    logger.info(f"  - Save to JSON: {not args.no_json}")
    logger.info(f"  - Save to MongoDB: {not args.no_mongodb}")
    logger.info(f"  - Build player profiles: {args.build_players}")
    logger.info(f"  - League limit: {args.league_limit or 'None (all leagues)'}")
    logger.info(f"  - Skip leagues: {skip_leagues if skip_leagues else 'None'}")
    logger.info(f"  - Force re-process: {args.force}")
    logger.info(f"  - Retry failed only: {args.retry_failed}")
    logger.info("=" * 60)
    
    run_pipeline(
        source=args.source,
        save_to_json=not args.no_json,
        save_to_mongodb=not args.no_mongodb,
        build_players=args.build_players,
        league_limit=args.league_limit,
        skip_leagues=skip_leagues,
        force=args.force,
        retry_failed_only=args.retry_failed,
        no_browser=args.no_browser,
        skip_individual_player_stats=args.skip_individual_player_stats
    )