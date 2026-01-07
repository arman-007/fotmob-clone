#!/usr/bin/env python3
"""
Daily Football Stats Pipeline

Fetches and stores daily match data and player statistics for incremental updates.
Designed to run once per day (typically end of day) to capture all completed matches.

Features:
    - Fetches all matches for a specific date (default: today)
    - Optional league filtering
    - Saves to MongoDB (same collections as historical pipeline)
    - Safe updates: won't overwrite existing data with empty responses
    - Optional JSON output for debugging
    - Token refresh every 35-40 matches

Usage:
    python daily_pipeline.py                          # Fetch today's matches
    python daily_pipeline.py -d 20241215              # Fetch specific date
    python daily_pipeline.py --leagues 47,55,87       # Only specific leagues
    python daily_pipeline.py --no-json                # Skip JSON output
    python daily_pipeline.py --finished-only          # Only finished matches
    python daily_pipeline.py --dry-run                # Show what would be processed
    python daily_pipeline.py --status                 # Show today's match summary

CLI Flags:
    -d, --date DATE         Date to fetch (YYYYMMDD format, default: today)
    --leagues IDS           Comma-separated league IDs to filter (default: all)
                            Example: --leagues 47,55,87
    --no-json               Skip saving JSON files
    --no-mongodb            Skip saving to MongoDB (JSON only)
    --finished-only         Only process finished matches
    --started-only          Only process started matches (includes in-progress)
    --match-limit N         Limit number of matches to process (for testing)
    --dry-run               Show what would be processed without actually processing
    --status                Show match summary for the date and exit
    --force                 Force update even if match exists (bypass safety checks)
    --output-dir DIR        Custom output directory for JSON files (default: output/daily)
    -v, --verbose           Enable verbose/debug logging

Examples:
    # Fetch all of today's finished matches
    python daily_pipeline.py --finished-only

    # Fetch matches from a specific date
    python daily_pipeline.py -d 20241215 --finished-only

    # Only process Premier League (47), La Liga (87), Bundesliga (54)
    python daily_pipeline.py --leagues 47,87,54 --finished-only

    # Preview what would be processed
    python daily_pipeline.py --dry-run

    # Check match summary for today
    python daily_pipeline.py --status

    # Test with 10 matches
    python daily_pipeline.py --match-limit 10

    # Skip JSON, only save to MongoDB
    python daily_pipeline.py --no-json --finished-only
"""

import argparse
import logging
import os
import random
import sys
import time
from datetime import datetime
from typing import List, Optional, Set

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from service.get_auth_headers import capture_x_mas
from service.get_daily_matches import fetch_matches_by_date, get_match_ids_from_json
from service.match_stats_processor import (
    fetch_match_details,
    process_match_response,
    save_match_to_mongodb,
    save_match_to_json
)

# MongoDB imports
try:
    from db import get_mongodb_service, MongoDBConfig
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    get_mongodb_service = None


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


def parse_league_ids(leagues_str: str) -> Optional[List[int]]:
    """Parse comma-separated league IDs to list of integers."""
    if not leagues_str:
        return None
    
    league_ids = []
    for lid in leagues_str.split(","):
        lid_int = safe_int(lid.strip())
        if lid_int:
            league_ids.append(lid_int)
    
    return league_ids if league_ids else None


# =============================================================================
# Logging Configuration
# =============================================================================

def setup_logging(verbose: bool = False):
    """Configure logging for the daily pipeline."""
    os.makedirs('logs', exist_ok=True)
    
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        handlers=[
            logging.FileHandler('logs/daily_pipeline.log', mode='a'),
            logging.StreamHandler(sys.stdout)
        ],
        format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s',
    )
    
    # Suppress noisy third-party loggers
    _suppress_noisy_loggers()
    
    return logging.getLogger(__name__)


def _suppress_noisy_loggers():
    """
    Suppress verbose logs from third-party libraries.
    
    Selenium-wire logs every HTTP request/response which creates
    hundreds of log lines just from loading one page.
    """
    noisy_loggers = [
        'seleniumwire.handler',
        'seleniumwire.server', 
        'seleniumwire.backend',
        'seleniumwire.storage',
        'seleniumwire',
        'urllib3',
        'hpack',
        'selenium.webdriver.remote.remote_connection',
    ]
    
    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)



# =============================================================================
# MongoDB Initialization
# =============================================================================

def initialize_mongodb() -> bool:
    """Initialize MongoDB connection."""
    if not MONGODB_AVAILABLE:
        logging.warning("MongoDB module not available")
        return False
    
    try:
        mongo = get_mongodb_service()
        mongo.connect()
        logging.info("✅ MongoDB connected successfully")
        return True
    except Exception as e:
        logging.error(f"❌ MongoDB connection failed: {e}")
        return False


# =============================================================================
# Status Display
# =============================================================================

def show_daily_status(date: str, league_ids: List[int] = None) -> None:
    """
    Display match summary for a given date.
    
    Shows:
    - Total matches scheduled
    - Matches by status (not started, in progress, finished)
    - Matches by league
    """
    print(f"\n{'='*60}")
    print(f"DAILY MATCH STATUS FOR {date}")
    print(f"{'='*60}")
    
    # Try to load from cached JSON first
    result = fetch_matches_by_date(
        date=date,
        league_ids=league_ids,
        save_to_json=True
    )
    
    if not result:
        print("❌ Failed to fetch match data")
        return
    
    matches = result["matches"]
    leagues = result["leagues"]
    
    print(f"\n📊 Summary:")
    print(f"   Total Leagues: {len(leagues)}")
    print(f"   Total Matches: {len(matches)}")
    
    # Count by status
    not_started = 0
    in_progress = 0
    finished = 0
    
    for match in matches:
        status = match.get("status", {})
        if status.get("finished"):
            finished += 1
        elif status.get("started"):
            in_progress += 1
        else:
            not_started += 1
    
    print(f"\n📈 By Status:")
    print(f"   ⏳ Not Started: {not_started}")
    print(f"   🔄 In Progress: {in_progress}")
    print(f"   ✅ Finished: {finished}")
    
    # Count by league (top 10)
    league_counts = {}
    for match in matches:
        lid = match.get("league_id")
        lname = match.get("league_name", "Unknown")
        if lid not in league_counts:
            league_counts[lid] = {"name": lname, "count": 0}
        league_counts[lid]["count"] += 1
    
    sorted_leagues = sorted(league_counts.items(), key=lambda x: x[1]["count"], reverse=True)[:10]
    
    print(f"\n🏆 Top Leagues (by match count):")
    for lid, info in sorted_leagues:
        print(f"   {info['name']}: {info['count']} matches")
    
    print(f"\n{'='*60}\n")


# =============================================================================
# Main Pipeline
# =============================================================================

def run_daily_pipeline(
    date: str,
    league_ids: List[int] = None,
    save_to_json: bool = True,
    save_to_mongodb: bool = True,
    finished_only: bool = False,
    started_only: bool = False,
    match_limit: int = None,
    dry_run: bool = False,
    force_update: bool = False,
    output_dir: str = "output/daily"
) -> dict:
    """
    Run the daily data pipeline.
    
    Args:
        date: Date string in YYYYMMDD format
        league_ids: Optional list of league IDs to filter (integers)
        save_to_json: Whether to save JSON files
        save_to_mongodb: Whether to save to MongoDB
        finished_only: Only process finished matches
        started_only: Only process started matches
        match_limit: Limit number of matches (for testing)
        dry_run: Show what would be processed without processing
        force_update: Force update even if match exists (bypass safety)
        output_dir: Output directory for JSON files
        
    Returns:
        Dict with processing statistics
    """
    logger = logging.getLogger(__name__)
    
    stats = {
        "date": date,
        "total_matches": 0,
        "processed": 0,
        "skipped": 0,
        "failed": 0,
        "mongo_matches": 0,
        "mongo_player_stats": 0,
        "start_time": time.time()
    }
    
    failed_matches = []  # Track failed match IDs with errors
    
    logger.info(f"{'='*60}")
    logger.info(f"DAILY PIPELINE - Processing date: {date}")
    logger.info(f"{'='*60}")
    logger.info(f"Configuration:")
    logger.info(f"  - Leagues filter: {league_ids or 'All leagues'}")
    logger.info(f"  - Save to JSON: {save_to_json}")
    logger.info(f"  - Save to MongoDB: {save_to_mongodb}")
    logger.info(f"  - Finished only: {finished_only}")
    logger.info(f"  - Started only: {started_only}")
    logger.info(f"  - Match limit: {match_limit or 'No limit'}")
    logger.info(f"  - Dry run: {dry_run}")
    logger.info(f"  - Force update: {force_update}")
    logger.info(f"{'='*60}")
    
    # =========================================================================
    # Step 1: Initialize MongoDB (if enabled)
    # =========================================================================
    if save_to_mongodb and not dry_run:
        if not initialize_mongodb():
            logger.error("MongoDB initialization failed, continuing with JSON only")
            save_to_mongodb = False
    
    # =========================================================================
    # Step 2: Capture X-MAS token
    # =========================================================================
    logger.info("Capturing X-MAS token...")
    x_mas = capture_x_mas()
    
    if not x_mas:
        logger.error("❌ Failed to capture X-MAS token. Exiting.")
        return stats
    
    logger.info(f"✅ X-MAS token captured: {x_mas[:30]}...")
    
    # =========================================================================
    # Step 3: Fetch matches for the date
    # =========================================================================
    logger.info(f"Fetching matches for {date}...")
    
    result = fetch_matches_by_date(
        date=date,
        x_mas=x_mas,
        league_ids=league_ids,
        save_to_json=save_to_json,
        output_dir=output_dir
    )
    
    if not result:
        logger.error(f"❌ Failed to fetch matches for {date}")
        return stats
    
    matches = result["matches"]
    stats["total_matches"] = len(matches)
    
    logger.info(f"Found {len(matches)} matches for {date}")
    
    # =========================================================================
    # Step 4: Filter matches by status
    # =========================================================================
    matches_to_process = []
    skipped_matches = []  # Track skipped match IDs with reasons
    
    for match in matches:
        status = match.get("status", {})
        match_id = safe_int(match.get("match_id"))  # Convert to int
        league_name = match.get("league_name", "Unknown")
        
        if not match_id:
            logger.warning(f"Skipping match with invalid ID: {match.get('match_id')}")
            continue
        
        if finished_only and not status.get("finished"):
            stats["skipped"] += 1
            skipped_matches.append({
                "match_id": match_id,
                "league": league_name,
                "reason": "not_finished"
            })
            continue
        
        if started_only and not status.get("started"):
            stats["skipped"] += 1
            skipped_matches.append({
                "match_id": match_id,
                "league": league_name,
                "reason": "not_started"
            })
            continue
        
        # Ensure match_id is int in the match dict
        match["match_id"] = match_id
        matches_to_process.append(match)
    
    logger.info(f"Matches to process after filtering: {len(matches_to_process)}")
    
    # Apply match limit
    if match_limit and len(matches_to_process) > match_limit:
        matches_to_process = matches_to_process[:match_limit]
        logger.info(f"Limited to {match_limit} matches")
    
    # =========================================================================
    # Step 5: Dry run - just show what would be processed
    # =========================================================================
    if dry_run:
        logger.info("\n🔍 DRY RUN - Would process the following matches:")
        for match in matches_to_process[:20]:  # Show first 20
            status = match.get("status", {})
            status_str = "✅" if status.get("finished") else ("🔄" if status.get("started") else "⏳")
            logger.info(f"  {status_str} {match['match_id']} - {match.get('league_name', 'Unknown')}")
        
        if len(matches_to_process) > 20:
            logger.info(f"  ... and {len(matches_to_process) - 20} more")
        
        stats["processed"] = len(matches_to_process)
        return stats
    
    # =========================================================================
    # Step 6: Process each match
    # =========================================================================
    iteration_count = 0
    token_refresh_threshold = random.randint(35, 40)
    
    for idx, match in enumerate(matches_to_process, 1):
        match_id = match["match_id"]  # Already int
        league_id = safe_int(match.get("league_id"))  # Convert to int
        
        logger.info(f"[{idx}/{len(matches_to_process)}] Processing match {match_id} ({match.get('league_name', 'Unknown')})")
        
        iteration_count += 1
        
        # Refresh token every 35-40 matches
        if iteration_count >= token_refresh_threshold:
            logger.info("Refreshing X-MAS token...")
            x_mas = capture_x_mas()
            if not x_mas:
                logger.error("❌ Failed to refresh X-MAS token")
                stats["failed"] += 1
                continue
            logger.info(f"✅ Token refreshed: {x_mas[:30]}...")
            iteration_count = 0
            token_refresh_threshold = random.randint(35, 40)
        
        # Fetch match details
        try:
            response_data = fetch_match_details(x_mas, match_id)
            
            if not response_data:
                logger.warning(f"No data returned for match {match_id}")
                stats["failed"] += 1
                failed_matches.append({
                    "match_id": match_id,
                    "league": match.get("league_name", "Unknown"),
                    "error": "No data returned from API"
                })
                continue
            
            # Process the response
            processed_data = process_match_response(response_data)
            
            if not processed_data:
                logger.warning(f"Failed to process match {match_id}")
                stats["failed"] += 1
                failed_matches.append({
                    "match_id": match_id,
                    "league": match.get("league_name", "Unknown"),
                    "error": "Failed to process response"
                })
                continue
            
            # Save to JSON
            if save_to_json:
                json_output_dir = f"{output_dir}/player_stats"
                save_match_to_json(processed_data, match_id, json_output_dir)
            
            # Save to MongoDB
            if save_to_mongodb:
                success, mongo_stats = save_match_to_mongodb(
                    match_data=processed_data,
                    league_id=league_id,  # Now int
                    safe_update=not force_update
                )
                
                if success:
                    stats["mongo_matches"] += mongo_stats.get("matches", 0)
                    stats["mongo_player_stats"] += mongo_stats.get("player_stats", 0)
                else:
                    logger.warning(f"MongoDB save issues for match {match_id}")
            
            stats["processed"] += 1
            
            # Progress logging
            if stats["processed"] % 25 == 0:
                logger.info(f"Progress: {stats['processed']} processed, {stats['failed']} failed")
                
        except Exception as e:
            logger.error(f"Error processing match {match_id}: {e}")
            stats["failed"] += 1
            failed_matches.append({
                "match_id": match_id,
                "league": match.get("league_name", "Unknown"),
                "error": str(e)[:100]  # Truncate long errors
            })
            continue
    
    # =========================================================================
    # Step 7: Summary
    # =========================================================================
    stats["end_time"] = time.time()
    stats["duration_seconds"] = stats["end_time"] - stats["start_time"]
    
    # Use print for clean summary output (no logger prefixes)
    print(f"\n{'='*60}")
    print("DAILY PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"Date: {date}")
    print(f"Total matches found: {stats['total_matches']}")
    print(f"Matches processed: {stats['processed']}")
    print(f"Matches skipped: {stats['skipped']}")
    print(f"Matches failed: {stats['failed']}")
    
    # Display skipped match IDs if any
    if skipped_matches:
        print(f"\n⏭️  Skipped Matches ({len(skipped_matches)}):")
        
        # Group by reason
        by_reason = {}
        for sm in skipped_matches:
            reason = sm["reason"]
            if reason not in by_reason:
                by_reason[reason] = []
            by_reason[reason].append(sm)
        
        for reason, matches_list in by_reason.items():
            reason_display = {
                "not_finished": "Not Finished",
                "not_started": "Not Started"
            }.get(reason, reason)
            
            print(f"\n   {reason_display} ({len(matches_list)}):")
            
            # Show first 15 matches, summarize rest
            for sm in matches_list[:15]:
                print(f"     - {sm['match_id']} ({sm['league']})")
            
            if len(matches_list) > 15:
                print(f"     ... and {len(matches_list) - 15} more")
    
    # Display failed match IDs if any
    if failed_matches:
        print(f"\n❌ Failed Matches ({len(failed_matches)}):")
        
        for fm in failed_matches[:15]:
            print(f"   - {fm['match_id']} ({fm['league']}): {fm['error']}")
        
        if len(failed_matches) > 15:
            print(f"   ... and {len(failed_matches) - 15} more")
    
    if save_to_mongodb:
        print(f"\nMongoDB - Matches saved: {stats['mongo_matches']}")
        print(f"MongoDB - Player stats saved: {stats['mongo_player_stats']}")
    
    print(f"\nDuration: {stats['duration_seconds']:.2f} seconds")
    print(f"{'='*60}\n")
    
    # Also log summary to file (single line for log parsing)
    logger.info(f"COMPLETE: date={date}, processed={stats['processed']}, skipped={stats['skipped']}, failed={stats['failed']}, duration={stats['duration_seconds']:.2f}s")
    
    return stats


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    """Main entry point for the daily pipeline."""
    parser = argparse.ArgumentParser(
        description="Daily Football Stats Pipeline - Fetch and store daily match data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python daily_pipeline.py                          # Fetch today's matches
    python daily_pipeline.py -d 20241215              # Fetch specific date
    python daily_pipeline.py --leagues 47,55,87       # Only specific leagues
    python daily_pipeline.py --no-json                # Skip JSON output
    python daily_pipeline.py --finished-only          # Only finished matches
    python daily_pipeline.py --dry-run                # Preview without processing
    python daily_pipeline.py --status                 # Show match summary
    python daily_pipeline.py --match-limit 10         # Test with 10 matches
        """
    )
    
    # Date argument
    parser.add_argument(
        "-d", "--date",
        type=str,
        default=datetime.now().strftime("%Y%m%d"),
        help="Date to fetch (YYYYMMDD format, default: today)"
    )
    
    # League filter
    parser.add_argument(
        "--leagues",
        type=str,
        default=None,
        help="Comma-separated league IDs to filter (e.g., '47,55,87')"
    )
    
    # Output options
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Skip saving JSON files"
    )
    
    parser.add_argument(
        "--no-mongodb",
        action="store_true",
        help="Skip saving to MongoDB"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output/daily",
        help="Output directory for JSON files (default: output/daily)"
    )
    
    # Match filtering
    parser.add_argument(
        "--finished-only",
        action="store_true",
        help="Only process finished matches"
    )
    
    parser.add_argument(
        "--started-only",
        action="store_true",
        help="Only process started matches (includes in-progress)"
    )
    
    parser.add_argument(
        "--match-limit",
        type=int,
        default=None,
        help="Limit number of matches to process (for testing)"
    )
    
    # Operation modes
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without actually processing"
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show match summary for the date and exit"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force update even if match exists (bypass safety checks)"
    )
    
    # Logging
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.verbose)
    
    # Parse league IDs (now returns List[int])
    league_ids = parse_league_ids(args.leagues)
    
    # Status mode
    if args.status:
        show_daily_status(args.date, league_ids)
        return
    
    # Run pipeline
    stats = run_daily_pipeline(
        date=args.date,
        league_ids=league_ids,
        save_to_json=not args.no_json,
        save_to_mongodb=not args.no_mongodb,
        finished_only=args.finished_only,
        started_only=args.started_only,
        match_limit=args.match_limit,
        dry_run=args.dry_run,
        force_update=args.force,
        output_dir=args.output_dir
    )
    
    # Exit code based on success
    if stats["failed"] > stats["processed"]:
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()