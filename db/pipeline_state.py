"""
Pipeline State Management

Provides checkpoint/resume functionality for the data pipeline.
Tracks processing status at season level with failed match tracking.

Collections:
- pipeline_state: Tracks league/season processing status
- pipeline_runs: Tracks overall pipeline run history

Usage:
    from db.pipeline_state import PipelineStateManager
    
    state_manager = PipelineStateManager()
    
    # Check if season needs processing
    if state_manager.should_process_season("47", "2024-2025"):
        # Process the season...
        state_manager.mark_season_in_progress("47", "2024-2025", total_matches=380)
        
        for match_id in match_ids:
            try:
                process_match(match_id)
                state_manager.record_match_processed("47", "2024-2025", match_id)
            except Exception as e:
                state_manager.record_match_failed("47", "2024-2025", match_id, str(e))
        
        state_manager.mark_season_completed("47", "2024-2025")
"""

import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum

from db.mongodb_service import get_mongodb_service, MongoDBService

logger = logging.getLogger(__name__)


class SeasonStatus(str, Enum):
    """Status values for season processing."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIALLY_COMPLETED = "partially_completed"  # Some matches failed


class PipelineStateManager:
    """
    Manages pipeline state for checkpoint/resume functionality.
    
    Tracks:
    - Which seasons have been processed
    - Which matches failed (for retry)
    - Processing statistics
    """
    
    def __init__(self, mongo_service: MongoDBService = None):
        self.mongo = mongo_service or get_mongodb_service()
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Create indexes for pipeline_state collection."""
        try:
            self.mongo.db.pipeline_state.create_index(
                [("league_id", 1), ("season_id", 1)],
                unique=True
            )
            self.mongo.db.pipeline_state.create_index([("status", 1)])
            self.mongo.db.pipeline_state.create_index([("last_updated", -1)])
            logger.debug("Pipeline state indexes created.")
        except Exception as e:
            logger.warning(f"Could not create pipeline_state indexes: {e}")
    
    @property
    def state_collection(self):
        """Get the pipeline_state collection."""
        return self.mongo.db.pipeline_state
    
    # =========================================================================
    # Season Status Methods
    # =========================================================================
    
    def get_season_state(self, league_id: str, season_id: str) -> Optional[dict]:
        """
        Get the current state of a season.
        
        Args:
            league_id: League ID
            season_id: Season ID
            
        Returns:
            State document or None if not found
        """
        return self.state_collection.find_one({
            "league_id": str(league_id),
            "season_id": str(season_id)
        })
    
    def should_process_season(
        self,
        league_id: str,
        season_id: str,
        force: bool = False
    ) -> Tuple[bool, List[str]]:
        """
        Check if a season should be processed.
        
        Args:
            league_id: League ID
            season_id: Season ID
            force: If True, always process (ignore completed status)
            
        Returns:
            Tuple of (should_process, match_ids_to_process)
            - If season is new: (True, []) - process all matches
            - If season has failures: (True, [failed_match_ids]) - retry failed only
            - If season is completed: (False, []) - skip
            - If force=True: (True, []) - process all matches
        """
        if force:
            return True, []
        
        state = self.get_season_state(league_id, season_id)
        
        if not state:
            # New season, needs full processing
            return True, []
        
        status = state.get("status")
        
        if status == SeasonStatus.COMPLETED:
            # Already done, skip
            logger.info(f"Season {league_id}_{season_id} already completed, skipping.")
            return False, []
        
        if status == SeasonStatus.PARTIALLY_COMPLETED:
            # Has failures, retry failed matches only
            failed_matches = state.get("failed_matches", [])
            if failed_matches:
                logger.info(f"Season {league_id}_{season_id} has {len(failed_matches)} failed matches to retry.")
                return True, [fm["match_id"] for fm in failed_matches]
            else:
                # No failures recorded, treat as completed
                return False, []
        
        if status == SeasonStatus.IN_PROGRESS:
            # Was interrupted, get unprocessed matches
            processed = set(state.get("processed_matches", []))
            failed = [fm["match_id"] for fm in state.get("failed_matches", [])]
            logger.info(f"Season {league_id}_{season_id} was interrupted. "
                    f"Processed: {len(processed)}, Failed: {len(failed)}")
            # Return failed matches to retry, caller will need to filter already processed
            return True, failed
        
        if status == SeasonStatus.FAILED:
            # Previous attempt failed completely, retry
            return True, []
        
        # Default: process
        return True, []
    
    def mark_season_in_progress(
        self,
        league_id: str,
        season_id: str,
        total_matches: int
    ):
        """
        Mark a season as in progress.
        
        Args:
            league_id: League ID
            season_id: Season ID
            total_matches: Total number of matches to process
        """
        now = datetime.now(timezone.utc)
        
        self.state_collection.update_one(
            {
                "league_id": str(league_id),
                "season_id": str(season_id)
            },
            {
                "$set": {
                    "status": SeasonStatus.IN_PROGRESS,
                    "total_matches": total_matches,
                    "last_updated": now
                },
                "$setOnInsert": {
                    "league_id": str(league_id),
                    "season_id": str(season_id),
                    "processed_matches": [],
                    "failed_matches": [],
                    "started_at": now
                }
            },
            upsert=True
        )
        
        logger.info(f"Season {league_id}_{season_id} marked as in_progress ({total_matches} matches)")
    
    def record_match_processed(
        self,
        league_id: str,
        season_id: str,
        match_id: str
    ):
        """
        Record that a match was successfully processed.
        
        Args:
            league_id: League ID
            season_id: Season ID
            match_id: Match ID that was processed
        """
        self.state_collection.update_one(
            {
                "league_id": str(league_id),
                "season_id": str(season_id)
            },
            {
                "$addToSet": {"processed_matches": str(match_id)},
                "$pull": {"failed_matches": {"match_id": str(match_id)}},
                "$set": {"last_updated": datetime.now(timezone.utc)}
            }
        )
    
    def record_match_failed(
        self,
        league_id: str,
        season_id: str,
        match_id: str,
        error: str = None
    ):
        """
        Record that a match failed processing.
        
        Args:
            league_id: League ID
            season_id: Season ID
            match_id: Match ID that failed
            error: Error message (optional)
        """
        failure_record = {
            "match_id": str(match_id),
            "error": error or "Unknown error",
            "failed_at": datetime.now(timezone.utc)
        }
        
        # Remove from failed_matches first (in case of retry), then add updated record
        self.state_collection.update_one(
            {
                "league_id": str(league_id),
                "season_id": str(season_id)
            },
            {
                "$pull": {"failed_matches": {"match_id": str(match_id)}}
            }
        )
        
        self.state_collection.update_one(
            {
                "league_id": str(league_id),
                "season_id": str(season_id)
            },
            {
                "$push": {"failed_matches": failure_record},
                "$set": {"last_updated": datetime.now(timezone.utc)}
            }
        )
    
    def mark_season_completed(
        self,
        league_id: str,
        season_id: str
    ):
        """
        Mark a season as completed.
        
        If there are failed matches, marks as PARTIALLY_COMPLETED instead.
        """
        state = self.get_season_state(league_id, season_id)
        
        failed_count = len(state.get("failed_matches", [])) if state else 0
        processed_count = len(state.get("processed_matches", [])) if state else 0
        
        if failed_count > 0:
            status = SeasonStatus.PARTIALLY_COMPLETED
        else:
            status = SeasonStatus.COMPLETED
        
        now = datetime.now(timezone.utc)
        
        self.state_collection.update_one(
            {
                "league_id": str(league_id),
                "season_id": str(season_id)
            },
            {
                "$set": {
                    "status": status,
                    "completed_at": now,
                    "last_updated": now
                }
            }
        )
        
        logger.info(f"Season {league_id}_{season_id} marked as {status}. "
                f"Processed: {processed_count}, Failed: {failed_count}")
    
    def mark_season_failed(
        self,
        league_id: str,
        season_id: str,
        error: str = None
    ):
        """Mark a season as failed (e.g., couldn't fetch season data)."""
        self.state_collection.update_one(
            {
                "league_id": str(league_id),
                "season_id": str(season_id)
            },
            {
                "$set": {
                    "status": SeasonStatus.FAILED,
                    "error": error,
                    "last_updated": datetime.now(timezone.utc)
                },
                "$setOnInsert": {
                    "league_id": str(league_id),
                    "season_id": str(season_id),
                    "started_at": datetime.now(timezone.utc)
                }
            },
            upsert=True
        )
    
    # =========================================================================
    # Query Methods
    # =========================================================================
    
    def get_pending_seasons(self, league_id: str = None) -> List[dict]:
        """Get all seasons that need processing."""
        query = {
            "status": {"$in": [
                SeasonStatus.PENDING,
                SeasonStatus.IN_PROGRESS,
                SeasonStatus.PARTIALLY_COMPLETED,
                SeasonStatus.FAILED
            ]}
        }
        
        if league_id:
            query["league_id"] = str(league_id)
        
        return list(self.state_collection.find(query))
    
    def get_completed_seasons(self, league_id: str = None) -> List[dict]:
        """Get all completed seasons."""
        query = {"status": SeasonStatus.COMPLETED}
        
        if league_id:
            query["league_id"] = str(league_id)
        
        return list(self.state_collection.find(query))
    
    def get_failed_matches(self, league_id: str = None) -> List[dict]:
        """Get all failed matches across all seasons."""
        query = {
            "failed_matches": {"$exists": True, "$ne": []}
        }
        
        if league_id:
            query["league_id"] = str(league_id)
        
        results = []
        for state in self.state_collection.find(query):
            for failure in state.get("failed_matches", []):
                results.append({
                    "league_id": state["league_id"],
                    "season_id": state["season_id"],
                    "match_id": failure["match_id"],
                    "error": failure.get("error"),
                    "failed_at": failure.get("failed_at")
                })
        
        return results
    
    def get_progress_summary(self) -> dict:
        """Get overall pipeline progress summary."""
        pipeline = [
            {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1},
                    "total_matches": {"$sum": "$total_matches"},
                    "processed_matches": {"$sum": {"$size": {"$ifNull": ["$processed_matches", []]}}}
                }
            }
        ]
        
        results = list(self.state_collection.aggregate(pipeline))
        
        summary = {
            "by_status": {r["_id"]: r["count"] for r in results},
            "total_seasons": sum(r["count"] for r in results),
            "total_matches": sum(r.get("total_matches", 0) for r in results),
            "processed_matches": sum(r.get("processed_matches", 0) for r in results)
        }
        
        # Get failed match count
        failed_matches = self.state_collection.aggregate([
            {"$unwind": {"path": "$failed_matches", "preserveNullAndEmptyArrays": False}},
            {"$count": "total"}
        ])
        failed_list = list(failed_matches)
        summary["failed_matches"] = failed_list[0]["total"] if failed_list else 0
        
        return summary
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def reset_season(self, league_id: str, season_id: str):
        """Reset a season's state (for re-processing)."""
        self.state_collection.delete_one({
            "league_id": str(league_id),
            "season_id": str(season_id)
        })
        logger.info(f"Reset state for season {league_id}_{season_id}")
    
    def reset_all(self, confirm: bool = False):
        """Reset all pipeline state. Use with caution!"""
        if not confirm:
            logger.warning("reset_all called without confirmation. Skipping.")
            return
        
        self.state_collection.delete_many({})
        logger.info("All pipeline state has been reset.")
    
    def is_match_processed(
        self,
        league_id: str,
        season_id: str,
        match_id: str
    ) -> bool:
        """Check if a specific match has been processed."""
        state = self.get_season_state(league_id, season_id)
        if not state:
            return False
        
        return str(match_id) in state.get("processed_matches", [])


# =============================================================================
# Convenience Function
# =============================================================================

def get_pipeline_state_manager(mongo_service: MongoDBService = None) -> PipelineStateManager:
    """Get PipelineStateManager instance."""
    return PipelineStateManager(mongo_service)