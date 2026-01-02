"""
Football Stats Pipeline

Main orchestration script for fetching and storing football statistics.

Storage Options:
- JSON files (for debugging/backup) - can be disabled via flags
- MongoDB (primary storage) - recommended for production

Features:
- Checkpoint/resume: Automatically resumes from where it stopped
- Failed match retry: Retries only failed matches on subsequent runs
- Progress tracking: Tracks processing status in MongoDB

Usage:
    python pipeline.py                    # Default: save to both JSON and MongoDB
    python pipeline.py --no-json          # Skip JSON files (faster)
    python pipeline.py --no-mongodb       # Skip MongoDB (debugging only)
    python pipeline.py --build-players    # Build aggregated player profiles after ingestion
    python pipeline.py --force            # Force re-process even completed seasons
    python pipeline.py --retry-failed     # Only retry failed matches
    python pipeline.py --status           # Show pipeline progress status
"""

import argparse
import logging
import time
import os
import glob
import sys
from datetime import datetime
from datetime import timezone
# Service imports
from service.get_auth_headers import capture_x_mas
from service.get_leagues import get_all_leagues
from service.get_specific_league import get_specific_league_data
from service.get_player_stats import get_match_wise_player_stats
from utils.get_all_season_match_ids import get_all_match_ids

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

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler('logs/pipeline_log.txt', mode='a'),
        logging.StreamHandler(sys.stdout)  # Also log to console
    ],
    format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s',
)


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
                    "league_id": entry.get("league_id", ""),
                    "season_id": entry.get("season_id", ""),
                    "league_season_key": key,
                    "team_id": entry.get("team_id", ""),
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
    save_to_json: bool = True,
    save_to_mongodb: bool = True,
    build_players: bool = False,
    league_limit: int = None,
    force: bool = False,
    retry_failed_only: bool = False
):
    """
    Run the complete data pipeline with checkpoint/resume support.
    
    Args:
        save_to_json: Whether to save JSON files (for debugging)
        save_to_mongodb: Whether to save to MongoDB (primary storage)
        build_players: Whether to build player profiles after ingestion
        league_limit: Limit number of leagues to process (for testing)
        force: Force re-process even completed seasons
        retry_failed_only: Only retry failed matches, skip successful ones
    """
    start_time = time.time()
    
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
    
    # Capture X-MAS token once and reuse throughout the pipeline
    logger.info("Capturing X-MAS token...")
    x_mas_token = capture_x_mas()
    if x_mas_token:
        logger.info(f"✅ X-MAS token captured: {x_mas_token[:30]}...")
    else:
        logger.error("❌ Failed to capture X-MAS token. Exiting.")
        return
    
    league_data = get_all_leagues(
        save_to_json=save_to_json,
        save_to_mongodb=save_to_mongodb
    )
    
    if not league_data:
        logger.error("Failed to fetch league data. Exiting.")
        return
    
    # # popular or international leagues
    # popular_leagues = league_data.get("international", [])

    # all leagues
    popular_leagues = league_data.get("countries", [])

    # this portion is only for country leagues
    popular_league_ids = [
        league['id'] 
        for country in popular_leagues
        for league in country['leagues']
    ]

    # # this portion is only for popular and international leagues
    # popular_league_ids = [str(league.get("id")) for league in popular_leagues]
    
    # Apply league limit if specified
    if league_limit:
        popular_league_ids = popular_league_ids[:league_limit]
        logger.info(f"Limited to {league_limit} leagues for processing")
    
    logger.info(f"Found {len(popular_league_ids)} popular leagues to process")
    
    # =========================================================================
    # Step 2: Process each league
    # =========================================================================
    total_matches_processed = 0
    total_matches_skipped = 0
    total_matches_failed = 0

    leagues_to_skip = ["10913", 285, 9173, 10175, 516, 112, 8965, 9213, 9305, 9381, 9170, 10007, 10832, 10075, 10053, 118, 113, 9495, 8938, 9471, 38, 119, 278,
            262, 10443, 263, 9255, 9521, 9658, 40, 149, 41, 264, 266, 144, 9334, 267, 268, 8814, 8971, 9067, 9429, 10077, 10290, 10272, 10274, 10291, 10273,
            10244, 10078, 9464, 270, 271, 9584, 9096, 272, 9986, 9837, 10872, 273, 9091, 9126, 9407, 120, 9137, 9550, 9491, 9490, 274, 9125, 121, 10223, 252,
            275, 276, 136, 9100, 330, 521, 122, 10025, 253, 46, 85, 239, 240, 241, 242, 256, 10046,246,11035,519,9941,10270,10314,335,47,48,108,109,117,8944,
            8947,9084,10176,247,132,133,10626,142,9253,10068,9227,10082,9717,9294,10844,10705,248,10034,9069,9523,250,51,52,251,143,8969,10174,10186,
            342,10713,53,110,8970,134,11028,9666,9667,9677,207,439,9310,54,146,208,209,512,9081,9734,10022,8924,9676,10840,10650,11034,522,135,145,
            8816,8815,336,337,212,213,215,216,217,10009,10226,10076,9478,10309,8982,10366,8983,10059,523,9372,9487,10288,524,10310,126,221,218,219,
            10307,9431,10210,127,128,9735,9097,9862,9098,55,86,141,147,222,11014,10178,10434,11015,#223,8974,9136,9011,10716,224,440,9500,225,9504,529,
            #226,9486,228,9632,9493,229,9527,9174,249,9528,8985,230,8976,11039,9906,
    ]
    
    for league_idx, league_id in enumerate(popular_league_ids, 1):
        logger.info(f"{'='*30} Processing League {league_idx}/{len(popular_league_ids)}: {league_id} {'='*30}")
        
        if league_id in leagues_to_skip:
            logger.warning(f"Skipping league {league_id}")
            continue
        
        # Fetch league/season data - pass the X-MAS token
        season_data = get_specific_league_data(
            league_id,
            x_mas=x_mas_token,
            save_to_json=save_to_json,
            save_to_mongodb=save_to_mongodb
        )
        
        if not season_data:
            logger.warning(f"Failed to fetch season data for league {league_id}")
            continue
        
        # Find all season JSON files for this league
        season_files_paths = glob.glob(
            f"output/leagues/{league_id}/**/league_info_*.json",
            recursive=True
        )
        
        logger.info(f"Found {len(season_files_paths)} season files for league {league_id}")
        
        # =====================================================================
        # Step 3: Process each season
        # =====================================================================
        for season_file_path in season_files_paths:
            # Extract season_id from path
            from db import extract_season_from_path
            season_id = extract_season_from_path(season_file_path)
            
            if not season_id:
                logger.warning(f"Could not extract season_id from {season_file_path}")
                continue
            
            logger.info(f"Processing season: {league_id}_{season_id}")
            
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
            
            # Capture fresh X-MAS token for each season
            logger.info("Refreshing X-MAS token for this season...")
            x_mas_token = capture_x_mas()
            
            if not x_mas_token:
                logger.error("Failed to capture X-MAS token. Skipping season.")
                if state_manager:
                    state_manager.mark_season_failed(league_id, season_id, "Failed to capture X-MAS token")
                continue
            
            logger.info(f"✅ X-MAS token refreshed: {x_mas_token[:30]}...")
            
            # Get match IDs for this season
            all_match_ids = get_all_match_ids(season_file_path)
            
            if not all_match_ids:
                logger.warning(f"No match IDs found in {season_file_path}")
                continue
            
            # Determine which matches to process
            if matches_to_retry:
                # Only retry failed matches
                match_ids = [mid for mid in all_match_ids if str(mid) in matches_to_retry]
                logger.info(f"Processing {len(match_ids)} failed matches (out of {len(all_match_ids)} total)")
            elif retry_failed_only and state_manager:
                # Skip already processed matches
                match_ids = [
                    mid for mid in all_match_ids 
                    if not state_manager.is_match_processed(league_id, season_id, str(mid))
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
                match_id_str = str(match_id)
                
                # Skip if already processed (unless force)
                if state_manager and not force and not matches_to_retry:
                    if state_manager.is_match_processed(league_id, season_id, match_id_str):
                        total_matches_skipped += 1
                        continue
                
                try:
                    result = get_match_wise_player_stats(
                        x_mas=x_mas_token,
                        match_id=match_id_str,
                        season_file_path=season_file_path,
                        save_to_json=save_to_json,
                        save_to_mongodb=save_to_mongodb
                    )
                    
                    if result:
                        total_matches_processed += 1
                        season_processed += 1
                        
                        # Record success
                        if state_manager:
                            state_manager.record_match_processed(league_id, season_id, match_id_str)
                    else:
                        total_matches_failed += 1
                        season_failed += 1
                        
                        # Record failure
                        if state_manager:
                            state_manager.record_match_failed(
                                league_id, season_id, match_id_str, 
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
                        state_manager.record_match_failed(league_id, season_id, match_id_str, str(e))
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
        description="Football Stats Pipeline - Fetch and store football statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python pipeline.py                     # Default: resume from where it stopped
    python pipeline.py --no-json           # Skip JSON files (faster, production mode)
    python pipeline.py --status            # Show pipeline progress status
    python pipeline.py --force             # Force re-process all (ignore checkpoints)
    python pipeline.py --retry-failed      # Only retry previously failed matches
    python pipeline.py --league-limit 2    # Process only first 2 leagues (testing)
    python pipeline.py --build-players     # Build player profiles after ingestion
        """
    )
    
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
    
    parser.add_argument(
        "--league-limit",
        type=int,
        default=None,
        help="Limit number of leagues to process (for testing)"
    )
    
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
    
    args = parser.parse_args()
    
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
    
    logger.info("=" * 60)
    logger.info("FOOTBALL STATS PIPELINE STARTED")
    logger.info("=" * 60)
    logger.info(f"Configuration:")
    logger.info(f"  - Save to JSON: {not args.no_json}")
    logger.info(f"  - Save to MongoDB: {not args.no_mongodb}")
    logger.info(f"  - Build player profiles: {args.build_players}")
    logger.info(f"  - League limit: {args.league_limit or 'None (all leagues)'}")
    logger.info(f"  - Force re-process: {args.force}")
    logger.info(f"  - Retry failed only: {args.retry_failed}")
    logger.info("=" * 60)
    
    run_pipeline(
        save_to_json=not args.no_json,
        save_to_mongodb=not args.no_mongodb,
        build_players=args.build_players,
        league_limit=args.league_limit,
        force=args.force,
        retry_failed_only=args.retry_failed
    )