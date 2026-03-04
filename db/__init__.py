"""
Database Module for Football Stats Platform

This module provides MongoDB integration with:
- Data validation (validators.py)
- Core database operations (mongodb_service.py)
- Query helpers for Fantasy PL style queries (query_helpers.py)
- Pipeline state management for checkpoint/resume (pipeline_state.py)

Updated: Integer IDs version
- All IDs (league_id, match_id, player_id, team_id) are now integers
- Removed player_match_key (using compound unique index instead)
"""

from db.mongodb_service import (
    MongoDBService,
    MongoDBConfig,
    get_mongodb_service,
    parse_datetime,
    safe_int,
    safe_float
)

from db.validators import (
    LeagueValidator,
    SeasonValidator,
    MatchValidator,
    PlayerStatValidator,
    PlayerMatchStatValidator,
    TeamValidator,
    ValidationResult,
    validate_league,
    validate_season,
    validate_match,
    validate_player_stat,
    validate_player_match_stat,
    validate_team
)

from db.query_helpers import (
    QueryHelpers,
    get_query_helpers
)

from db.pipeline_state import (
    PipelineStateManager,
    SeasonStatus,
    get_pipeline_state_manager
)

__all__ = [
    # MongoDB Service
    "MongoDBService",
    "MongoDBConfig", 
    "get_mongodb_service",
    
    # Validators
    "LeagueValidator",
    "SeasonValidator",
    "MatchValidator",
    "PlayerStatValidator",
    "PlayerMatchStatValidator",
    "TeamValidator",
    "ValidationResult",
    "validate_league",
    "validate_season",
    "validate_match",
    "validate_player_stat",
    "validate_player_match_stat",
    "validate_team",
    
    # Query Helpers
    "QueryHelpers",
    "get_query_helpers",
    
    # Pipeline State
    "PipelineStateManager",
    "SeasonStatus",
    "get_pipeline_state_manager",
    
    # Utilities
    "parse_datetime",
    "safe_int",
    "safe_float"
]
