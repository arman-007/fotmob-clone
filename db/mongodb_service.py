"""
MongoDB Service Module for Football Stats Platform

Core database operations including:
- Connection management
- Index creation
- CRUD operations for all collections
- Batch operations for efficient ingestion
"""

import os
import logging
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager

from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT, UpdateOne
from pymongo.errors import (
    DuplicateKeyError, 
    BulkWriteError, 
    ConnectionFailure,
    ServerSelectionTimeoutError
)
from pymongo.database import Database
from pymongo.collection import Collection
from dotenv import load_dotenv

from db.validators import (
    LeagueValidator,
    SeasonValidator,
    MatchValidator,
    PlayerMatchStatValidator,
    TeamValidator,
    ValidationResult,
    validate_league,
    validate_season,
    validate_match,
    validate_player_match_stat,
    validate_team
)

load_dotenv()

logger = logging.getLogger(__name__)


class MongoDBConfig:
    """Configuration class for MongoDB connection settings."""
    
    def __init__(
        self,
        uri: str = None,
        database_name: str = "football_stats",
        max_pool_size: int = 50,
        min_pool_size: int = 10,
        server_selection_timeout_ms: int = 5000,
        connect_timeout_ms: int = 10000
    ):
        self.uri = uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self.database_name = database_name
        self.max_pool_size = max_pool_size
        self.min_pool_size = min_pool_size
        self.server_selection_timeout_ms = server_selection_timeout_ms
        self.connect_timeout_ms = connect_timeout_ms


class MongoDBService:
    """
    Service class for MongoDB operations.
    
    Handles connection management, index creation, and provides
    methods for CRUD operations on all collections.
    """
    
    _instance = None
    _client: Optional[MongoClient] = None
    _db: Optional[Database] = None
    
    def __new__(cls, config: MongoDBConfig = None):
        """Singleton pattern to ensure single connection pool."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config: MongoDBConfig = None):
        if self._initialized:
            return
        
        self.config = config or MongoDBConfig()
        self._initialized = True
        self._connected = False
    
    @property
    def client(self) -> MongoClient:
        if not self._connected:
            self.connect()
        return self._client
    
    @property
    def db(self) -> Database:
        if not self._connected:
            self.connect()
        return self._db
    
    # =========================================================================
    # Collection Properties
    # =========================================================================
    
    @property
    def leagues(self) -> Collection:
        return self.db.leagues
    
    @property
    def seasons(self) -> Collection:
        return self.db.seasons
    
    @property
    def matches(self) -> Collection:
        return self.db.matches
    
    @property
    def player_stats(self) -> Collection:
        return self.db.player_stats
    
    @property
    def players(self) -> Collection:
        return self.db.players
    
    @property
    def teams(self) -> Collection:
        return self.db.teams
    
    # =========================================================================
    # Connection Management
    # =========================================================================
    
    def connect(self) -> bool:
        """
        Establish connection to MongoDB.
        
        Returns:
            bool: True if connection successful, False otherwise.
        """
        if self._connected:
            return True
        
        try:
            self._client = MongoClient(
                self.config.uri,
                maxPoolSize=self.config.max_pool_size,
                minPoolSize=self.config.min_pool_size,
                serverSelectionTimeoutMS=self.config.server_selection_timeout_ms,
                connectTimeoutMS=self.config.connect_timeout_ms
            )
            
            # Test connection
            self._client.admin.command('ping')
            
            self._db = self._client[self.config.database_name]
            self._connected = True
            
            logger.info(f"✅ Connected to MongoDB: {self.config.database_name}")
            return True
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            self._connected = False
            raise
    
    def disconnect(self):
        """Close MongoDB connection."""
        if self._client:
            self._client.close()
            self._connected = False
            logger.info("MongoDB connection closed.")
    
    def is_connected(self) -> bool:
        """Check if connected to MongoDB."""
        if not self._connected or not self._client:
            return False
        try:
            self._client.admin.command('ping')
            return True
        except Exception:
            self._connected = False
            return False
    
    @contextmanager
    def session(self):
        """Context manager for MongoDB sessions (for transactions)."""
        session = self.client.start_session()
        try:
            yield session
        finally:
            session.end_session()
    
    # =========================================================================
    # Index Management
    # =========================================================================
    
    def create_indexes(self):
        """Create all required indexes for optimal query performance."""
        logger.info("Creating indexes...")
        
        try:
            self._create_leagues_indexes()
            self._create_seasons_indexes()
            self._create_matches_indexes()
            self._create_player_stats_indexes()
            self._create_players_indexes()
            self._create_teams_indexes()
            
            logger.info("✅ All indexes created successfully.")
            
        except Exception as e:
            logger.error(f"❌ Error creating indexes: {e}")
            raise
    
    def _create_leagues_indexes(self):
        """Create indexes for leagues collection."""
        self.leagues.create_index([("league_id", ASCENDING)], unique=True)
        self.leagues.create_index([("country_code", ASCENDING)])
        self.leagues.create_index([("name", TEXT)])
        logger.debug("Leagues indexes created.")
    
    def _create_seasons_indexes(self):
        """Create indexes for seasons collection."""
        self.seasons.create_index([("league_season_key", ASCENDING)], unique=True)
        self.seasons.create_index([("league_id", ASCENDING), ("season_id", DESCENDING)])
        self.seasons.create_index([("season_id", ASCENDING)])
        logger.debug("Seasons indexes created.")
    
    def _create_matches_indexes(self):
        """Create indexes for matches collection."""
        self.matches.create_index([("match_id", ASCENDING)], unique=True)
        self.matches.create_index([
            ("league_id", ASCENDING),
            ("season_id", DESCENDING),
            ("match_datetime_utc", DESCENDING)
        ])
        self.matches.create_index([
            ("league_season_key", ASCENDING),
            ("match_datetime_utc", DESCENDING)
        ])
        self.matches.create_index([
            ("home_team.team_id", ASCENDING),
            ("match_datetime_utc", DESCENDING)
        ])
        self.matches.create_index([
            ("away_team.team_id", ASCENDING),
            ("match_datetime_utc", DESCENDING)
        ])
        self.matches.create_index([("match_datetime_utc", DESCENDING)])
        self.matches.create_index([("player_stats.player_id", ASCENDING)])
        self.matches.create_index([("finished", ASCENDING), ("league_season_key", ASCENDING)])
        logger.debug("Matches indexes created.")
    
    def _create_player_stats_indexes(self):
        """Create indexes for player_stats collection."""
        self.player_stats.create_index([("player_match_key", ASCENDING)], unique=True)
        self.player_stats.create_index([
            ("player_id", ASCENDING),
            ("match_datetime_utc", DESCENDING)
        ])
        self.player_stats.create_index([
            ("player_id", ASCENDING),
            ("league_season_key", ASCENDING),
            ("match_datetime_utc", DESCENDING)
        ])
        self.player_stats.create_index([
            ("team_id", ASCENDING),
            ("match_datetime_utc", DESCENDING)
        ])
        self.player_stats.create_index([("league_season_key", ASCENDING), ("goals", DESCENDING)])
        self.player_stats.create_index([("league_season_key", ASCENDING), ("assists", DESCENDING)])
        self.player_stats.create_index([("league_season_key", ASCENDING), ("rating", DESCENDING)])
        self.player_stats.create_index([("match_id", ASCENDING)])
        self.player_stats.create_index([("name", TEXT)])
        logger.debug("Player stats indexes created.")
    
    def _create_players_indexes(self):
        """Create indexes for players collection."""
        self.players.create_index([("player_id", ASCENDING)], unique=True)
        self.players.create_index([("current_team_id", ASCENDING)])
        self.players.create_index([("name", TEXT)])
        self.players.create_index([("total_goals", DESCENDING)])
        logger.debug("Players indexes created.")
    
    def _create_teams_indexes(self):
        """Create indexes for teams collection."""
        self.teams.create_index([("team_id", ASCENDING)], unique=True)
        self.teams.create_index([("name", TEXT)])
        logger.debug("Teams indexes created.")
    
    # =========================================================================
    # League Operations
    # =========================================================================
    
    def insert_league(self, league_data: dict, validate: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Insert or update a league document.
        
        Args:
            league_data: League data dictionary
            validate: Whether to validate before insertion
            
        Returns:
            Tuple of (success, error_message)
        """
        try:
            if validate:
                result = validate_league(league_data)
                if not result.is_valid:
                    return False, f"Validation failed: {result.errors}"
                league_data = result.data
            
            now = datetime.now(timezone.utc)
            league_data["updated_at"] = now
            
            self.leagues.update_one(
                {"league_id": league_data["league_id"]},
                {
                    "$set": league_data,
                    "$setOnInsert": {"created_at": now}
                },
                upsert=True
            )
            
            return True, None
            
        except Exception as e:
            logger.error(f"Error inserting league: {e}")
            return False, str(e)
    
    def insert_leagues_bulk(self, leagues: List[dict], validate: bool = True) -> Dict[str, int]:
        """
        Bulk insert/update leagues.
        
        Returns:
            Dict with counts: inserted, modified, errors
        """
        results = {"inserted": 0, "modified": 0, "errors": 0}
        
        operations = []
        now = datetime.now(timezone.utc)
        
        for league in leagues:
            try:
                if validate:
                    val_result = validate_league(league)
                    if not val_result.is_valid:
                        logger.warning(f"Invalid league data: {val_result.errors}")
                        results["errors"] += 1
                        continue
                    league = val_result.data
                
                league["updated_at"] = now
                
                operations.append(
                    UpdateOne(
                        {"league_id": league["league_id"]},
                        {
                            "$set": league,
                            "$setOnInsert": {"created_at": now}
                        },
                        upsert=True
                    )
                )
            except Exception as e:
                logger.warning(f"Error preparing league for bulk insert: {e}")
                results["errors"] += 1
        
        if operations:
            try:
                bulk_result = self.leagues.bulk_write(operations, ordered=False)
                results["inserted"] = bulk_result.upserted_count
                results["modified"] = bulk_result.modified_count
            except BulkWriteError as e:
                results["errors"] += len(e.details.get("writeErrors", []))
                logger.warning(f"Bulk write had errors: {e.details}")
        
        return results
    
    # =========================================================================
    # Season Operations
    # =========================================================================
    
    def insert_season(self, season_data: dict, validate: bool = True) -> Tuple[bool, Optional[str]]:
        """Insert or update a season document."""
        try:
            if validate:
                result = validate_season(season_data)
                if not result.is_valid:
                    return False, f"Validation failed: {result.errors}"
                season_data = result.data
            
            now = datetime.now(timezone.utc)
            season_data["updated_at"] = now
            
            self.seasons.update_one(
                {"league_season_key": season_data["league_season_key"]},
                {
                    "$set": season_data,
                    "$setOnInsert": {"created_at": now}
                },
                upsert=True
            )
            
            return True, None
            
        except Exception as e:
            logger.error(f"Error inserting season: {e}")
            return False, str(e)
    
    # =========================================================================
    # Match Operations
    # =========================================================================
    
    def insert_match(self, match_data: dict, validate: bool = True) -> Tuple[bool, Optional[str]]:
        """Insert or update a match document."""
        try:
            if validate:
                result = validate_match(match_data)
                if not result.is_valid:
                    return False, f"Validation failed: {result.errors}"
                # Use original data but ensure match_id is string
                match_data["match_id"] = str(match_data.get("match_id", ""))
            
            now = datetime.now(timezone.utc)
            match_data["updated_at"] = now
            
            self.matches.update_one(
                {"match_id": match_data["match_id"]},
                {
                    "$set": match_data,
                    "$setOnInsert": {"created_at": now}
                },
                upsert=True
            )
            
            return True, None
            
        except Exception as e:
            logger.error(f"Error inserting match: {e}")
            return False, str(e)
    
    # =========================================================================
    # Player Stats Operations
    # =========================================================================
    
    def insert_player_stat(self, stat_data: dict, validate: bool = True) -> Tuple[bool, Optional[str]]:
        """Insert or update a player match stat document."""
        try:
            if validate:
                result = validate_player_match_stat(stat_data)
                if not result.is_valid:
                    return False, f"Validation failed: {result.errors}"
                stat_data = result.data
            
            # Ensure player_match_key exists
            if not stat_data.get("player_match_key"):
                stat_data["player_match_key"] = f"{stat_data['player_id']}_{stat_data['match_id']}"
            
            now = datetime.now(timezone.utc)
            stat_data["created_at"] = now
            
            self.player_stats.update_one(
                {"player_match_key": stat_data["player_match_key"]},
                {"$set": stat_data},
                upsert=True
            )
            
            return True, None
            
        except Exception as e:
            logger.error(f"Error inserting player stat: {e}")
            return False, str(e)
    
    def insert_player_stats_bulk(self, stats: List[dict], validate: bool = True) -> Dict[str, int]:
        """
        Bulk insert player stats.
        
        Returns:
            Dict with counts: inserted, modified, errors
        """
        results = {"inserted": 0, "modified": 0, "errors": 0}
        
        operations = []
        now = datetime.now(timezone.utc)
        
        for stat in stats:
            try:
                if validate:
                    val_result = validate_player_match_stat(stat)
                    if not val_result.is_valid:
                        results["errors"] += 1
                        continue
                    stat = val_result.data
                
                if not stat.get("player_match_key"):
                    stat["player_match_key"] = f"{stat['player_id']}_{stat['match_id']}"
                
                stat["created_at"] = now
                
                operations.append(
                    UpdateOne(
                        {"player_match_key": stat["player_match_key"]},
                        {"$set": stat},
                        upsert=True
                    )
                )
            except Exception as e:
                logger.warning(f"Error preparing player stat: {e}")
                results["errors"] += 1
        
        if operations:
            try:
                bulk_result = self.player_stats.bulk_write(operations, ordered=False)
                results["inserted"] = bulk_result.upserted_count
                results["modified"] = bulk_result.modified_count
            except BulkWriteError as e:
                results["errors"] += len(e.details.get("writeErrors", []))
                logger.warning(f"Bulk write errors: {e.details}")
        
        return results
    
    # =========================================================================
    # Team Operations
    # =========================================================================
    
    def insert_team(self, team_data: dict, validate: bool = True) -> Tuple[bool, Optional[str]]:
        """Insert or update a team document."""
        try:
            if validate:
                result = validate_team(team_data)
                if not result.is_valid:
                    return False, f"Validation failed: {result.errors}"
                team_data = result.data
            
            now = datetime.now(timezone.utc)
            
            self.teams.update_one(
                {"team_id": team_data["team_id"]},
                {
                    "$set": {
                        "team_id": team_data["team_id"],
                        "name": team_data["name"],
                        "updated_at": now
                    },
                    "$setOnInsert": {"created_at": now}
                },
                upsert=True
            )
            
            return True, None
            
        except Exception as e:
            logger.error(f"Error inserting team: {e}")
            return False, str(e)
    
    def insert_teams_bulk(self, teams: List[Tuple[str, str]]) -> Dict[str, int]:
        """
        Bulk insert teams from list of (team_id, team_name) tuples.
        """
        results = {"inserted": 0, "modified": 0, "errors": 0}
        
        operations = []
        now = datetime.now(timezone.utc)
        
        for team_id, team_name in teams:
            if not team_id:
                continue
            
            operations.append(
                UpdateOne(
                    {"team_id": str(team_id)},
                    {
                        "$set": {
                            "team_id": str(team_id),
                            "name": team_name or "Unknown Team",
                            "updated_at": now
                        },
                        "$setOnInsert": {"created_at": now}
                    },
                    upsert=True
                )
            )
        
        if operations:
            try:
                bulk_result = self.teams.bulk_write(operations, ordered=False)
                results["inserted"] = bulk_result.upserted_count
                results["modified"] = bulk_result.modified_count
            except BulkWriteError as e:
                results["errors"] += len(e.details.get("writeErrors", []))
        
        return results
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def get_collection_stats(self) -> Dict[str, int]:
        """Get document counts for all collections."""
        return {
            "leagues": self.leagues.count_documents({}),
            "seasons": self.seasons.count_documents({}),
            "matches": self.matches.count_documents({}),
            "player_stats": self.player_stats.count_documents({}),
            "players": self.players.count_documents({}),
            "teams": self.teams.count_documents({})
        }
    
    def clear_all_collections(self, confirm: bool = False):
        """
        Clear all collections. Use with caution!
        
        Args:
            confirm: Must be True to actually clear
        """
        if not confirm:
            logger.warning("Clear all collections called without confirmation. Skipping.")
            return
        
        self.leagues.delete_many({})
        self.seasons.delete_many({})
        self.matches.delete_many({})
        self.player_stats.delete_many({})
        self.players.delete_many({})
        self.teams.delete_many({})
        
        logger.info("All collections cleared.")


# =============================================================================
# Helper Functions
# =============================================================================

def get_mongodb_service(config: MongoDBConfig = None) -> MongoDBService:
    """Get or create MongoDB service instance."""
    return MongoDBService(config)


def extract_season_from_path(filepath: str) -> Optional[str]:
    """
    Extract season ID from filepath.
    
    Handles formats like:
    - 2024-2025 (full year range)
    - 2024-25 (short year range)
    - 2024 (single year, e.g., World Cup)
    
    Returns the season ID as found in the path.
    """
    # First try to match full year range (2024-2025)
    match = re.search(r'(\d{4}-\d{4})', filepath)
    if match:
        return match.group(1)
    
    # Then try short year range (2024-25)
    match = re.search(r'(\d{4}-\d{2})', filepath)
    if match:
        return match.group(1)
    
    # Finally try single year (2024)
    match = re.search(r'/(\d{4})/', filepath)
    if match:
        return match.group(1)
    
    return None


def parse_datetime(dt_string: str) -> Optional[datetime]:
    """Parse datetime string to datetime object."""
    if not dt_string:
        return None
    
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_string, fmt)
        except ValueError:
            continue
    
    return None


def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert value to int."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value: Any, default: float = None) -> Optional[float]:
    """Safely convert value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default