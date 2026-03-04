"""
Query Helpers for Football Stats Platform

Provides high-level query functions for common Fantasy Premier League style queries.
This module is separate from the pipeline and can be used by the web application.

Updated: Integer IDs version
- All IDs (league_id, match_id, player_id, team_id) are now integers
- Removed player_match_key references

Usage:
    from db.query_helpers import QueryHelpers
    
    queries = QueryHelpers()
    top_scorers = queries.get_top_scorers(47, "2024-25", limit=20)
    player_stats = queries.get_player_stats(961995, "47_2024-25")
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Union

from db.mongodb_service import get_mongodb_service, MongoDBService
from utils.converters import safe_int as _ensure_int

logger = logging.getLogger(__name__)


class QueryHelpers:
    """
    High-level query helpers for Fantasy PL style queries.
    
    All methods return lists or dicts ready for API responses.
    """
    
    def __init__(self, mongo_service: MongoDBService = None):
        self.mongo = mongo_service or get_mongodb_service()
    
    # =========================================================================
    # League Queries
    # =========================================================================
    
    def get_all_leagues(self, category: str = None) -> List[dict]:
        """
        Get all leagues, optionally filtered by category.
        
        Args:
            category: Filter by category ('popular', 'international', 'domestic')
            
        Returns:
            List of league documents
        """
        query = {}
        if category:
            query["category"] = category
        
        return list(self.mongo.leagues.find(
            query,
            {"_id": 0}
        ).sort("name", 1))
    
    def get_league_by_id(self, league_id: Union[int, str]) -> Optional[dict]:
        """Get a single league by ID."""
        league_id_int = _ensure_int(league_id)
        if league_id_int is None:
            return None
        
        return self.mongo.leagues.find_one(
            {"league_id": league_id_int},
            {"_id": 0}
        )
    
    def get_league_seasons(self, league_id: Union[int, str]) -> List[dict]:
        """Get all seasons for a league."""
        league_id_int = _ensure_int(league_id)
        if league_id_int is None:
            return []
        
        return list(self.mongo.seasons.find(
            {"league_id": league_id_int},
            {"_id": 0}
        ).sort("season_id", -1))
    
    # =========================================================================
    # Match Queries
    # =========================================================================
    
    def get_matches_for_league_season(
        self,
        league_id: Union[int, str],
        season_id: str,
        finished_only: bool = False,
        limit: int = 100,
        skip: int = 0
    ) -> List[dict]:
        """
        Get all matches for a specific league and season.
        
        Args:
            league_id: League ID (int or string)
            season_id: Season ID (e.g., "2024-25")
            finished_only: Only return finished matches
            limit: Maximum number of matches to return
            skip: Number of matches to skip (for pagination)
            
        Returns:
            List of match documents (without embedded player_stats for efficiency)
        """
        league_id_int = _ensure_int(league_id)
        if league_id_int is None:
            return []
        
        league_season_key = f"{league_id_int}_{season_id}"
        
        query = {"league_season_key": league_season_key}
        if finished_only:
            query["finished"] = True
        
        return list(self.mongo.matches.find(
            query,
            {
                "_id": 0,
                "player_stats": 0  # Exclude for list views
            }
        ).sort("match_datetime_utc", -1).skip(skip).limit(limit))
    
    def get_match_by_id(self, match_id: Union[int, str], include_player_stats: bool = True) -> Optional[dict]:
        """
        Get a single match by ID.
        
        Args:
            match_id: Match ID (int or string)
            include_player_stats: Whether to include embedded player_stats
            
        Returns:
            Match document or None
        """
        match_id_int = _ensure_int(match_id)
        if match_id_int is None:
            return None
        
        projection = {"_id": 0}
        if not include_player_stats:
            projection["player_stats"] = 0
        
        return self.mongo.matches.find_one(
            {"match_id": match_id_int},
            projection
        )
    
    def get_recent_matches(
        self,
        league_id: Union[int, str] = None,
        days: int = 7,
        limit: int = 20
    ) -> List[dict]:
        """
        Get recent matches from the last N days.
        
        Args:
            league_id: Optional league filter (int or string)
            days: Number of days to look back
            limit: Maximum results
            
        Returns:
            List of recent matches
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        query = {"match_datetime_utc": {"$gte": cutoff}}
        if league_id is not None:
            league_id_int = _ensure_int(league_id)
            if league_id_int is not None:
                query["league_id"] = league_id_int
        
        return list(self.mongo.matches.find(
            query,
            {"_id": 0, "player_stats": 0}
        ).sort("match_datetime_utc", -1).limit(limit))
    
    def get_team_matches(
        self,
        team_id: Union[int, str],
        league_season_key: str = None,
        limit: int = 20
    ) -> List[dict]:
        """
        Get matches for a specific team.
        
        Args:
            team_id: Team ID (int or string)
            league_season_key: Optional league/season filter
            limit: Maximum results
            
        Returns:
            List of matches involving the team
        """
        team_id_int = _ensure_int(team_id)
        if team_id_int is None:
            return []
        
        query = {
            "$or": [
                {"home_team.team_id": team_id_int},
                {"away_team.team_id": team_id_int}
            ]
        }
        
        if league_season_key:
            query["league_season_key"] = league_season_key
        
        return list(self.mongo.matches.find(
            query,
            {"_id": 0, "player_stats": 0}
        ).sort("match_datetime_utc", -1).limit(limit))
    
    # =========================================================================
    # Player Stats Queries (Core Fantasy PL Queries)
    # =========================================================================
    
    def get_player_stats(
        self,
        player_id: Union[int, str],
        league_season_key: str = None,
        limit: int = 50
    ) -> List[dict]:
        """
        Get all stats for a specific player.
        
        Args:
            player_id: Player ID (int or string)
            league_season_key: Optional league/season filter
            limit: Maximum results
            
        Returns:
            List of player match stats, sorted by date descending
        """
        player_id_int = _ensure_int(player_id)
        if player_id_int is None:
            return []
        
        query = {"player_id": player_id_int}
        
        if league_season_key:
            query["league_season_key"] = league_season_key
        
        return list(self.mongo.player_stats.find(
            query,
            {"_id": 0}
        ).sort("match_datetime_utc", -1).limit(limit))
    
    def get_player_form(self, player_id: Union[int, str], matches: int = 5) -> List[dict]:
        """
        Get player's recent form (last N matches).
        
        Args:
            player_id: Player ID (int or string)
            matches: Number of recent matches
            
        Returns:
            List of recent match stats
        """
        player_id_int = _ensure_int(player_id)
        if player_id_int is None:
            return []
        
        return list(self.mongo.player_stats.find(
            {"player_id": player_id_int},
            {
                "_id": 0,
                "match_id": 1,
                "match_datetime_utc": 1,
                "opponent_team_name": 1,
                "is_home": 1,
                "goals": 1,
                "assists": 1,
                "rating": 1,
                "minutes_played": 1,
                "yellow_card": 1,
                "red_card": 1
            }
        ).sort("match_datetime_utc", -1).limit(matches))
    
    def get_match_player_stats(self, match_id: Union[int, str]) -> List[dict]:
        """
        Get all player stats for a specific match.
        
        Args:
            match_id: Match ID (int or string)
            
        Returns:
            List of player stats for the match
        """
        match_id_int = _ensure_int(match_id)
        if match_id_int is None:
            return []
        
        return list(self.mongo.player_stats.find(
            {"match_id": match_id_int},
            {"_id": 0}
        ).sort("team_id", 1))
    
    def get_top_scorers(
        self,
        league_id: Union[int, str],
        season_id: str,
        limit: int = 20
    ) -> List[dict]:
        """
        Get top scorers for a league/season.
        
        Args:
            league_id: League ID (int or string)
            season_id: Season ID
            limit: Number of top scorers to return
            
        Returns:
            List of players with aggregated goal stats
        """
        league_id_int = _ensure_int(league_id)
        if league_id_int is None:
            return []
        
        league_season_key = f"{league_id_int}_{season_id}"
        
        pipeline = [
            {"$match": {"league_season_key": league_season_key}},
            {
                "$group": {
                    "_id": "$player_id",
                    "name": {"$first": "$name"},
                    "team_id": {"$last": "$team_id"},
                    "team_name": {"$last": "$team_name"},
                    "total_goals": {"$sum": "$goals"},
                    "total_assists": {"$sum": "$assists"},
                    "matches_played": {"$sum": 1},
                    "total_minutes": {"$sum": "$minutes_played"},
                    "avg_rating": {"$avg": "$rating"}
                }
            },
            {"$sort": {"total_goals": -1, "total_assists": -1}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "player_id": "$_id",
                    "name": 1,
                    "team_id": 1,
                    "team_name": 1,
                    "total_goals": 1,
                    "total_assists": 1,
                    "matches_played": 1,
                    "total_minutes": 1,
                    "avg_rating": {"$round": ["$avg_rating", 2]}
                }
            }
        ]
        
        return list(self.mongo.player_stats.aggregate(pipeline))
    
    def get_top_assists(
        self,
        league_id: Union[int, str],
        season_id: str,
        limit: int = 20
    ) -> List[dict]:
        """
        Get top assist providers for a league/season.
        """
        league_id_int = _ensure_int(league_id)
        if league_id_int is None:
            return []
        
        league_season_key = f"{league_id_int}_{season_id}"
        
        pipeline = [
            {"$match": {"league_season_key": league_season_key}},
            {
                "$group": {
                    "_id": "$player_id",
                    "name": {"$first": "$name"},
                    "team_id": {"$last": "$team_id"},
                    "team_name": {"$last": "$team_name"},
                    "total_assists": {"$sum": "$assists"},
                    "total_goals": {"$sum": "$goals"},
                    "matches_played": {"$sum": 1},
                    "avg_rating": {"$avg": "$rating"}
                }
            },
            {"$sort": {"total_assists": -1, "total_goals": -1}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "player_id": "$_id",
                    "name": 1,
                    "team_id": 1,
                    "team_name": 1,
                    "total_assists": 1,
                    "total_goals": 1,
                    "matches_played": 1,
                    "avg_rating": {"$round": ["$avg_rating", 2]}
                }
            }
        ]
        
        return list(self.mongo.player_stats.aggregate(pipeline))
    
    def get_top_rated_players(
        self,
        league_id: Union[int, str],
        season_id: str,
        min_matches: int = 5,
        limit: int = 20
    ) -> List[dict]:
        """
        Get top rated players for a league/season.
        
        Args:
            league_id: League ID (int or string)
            season_id: Season ID
            min_matches: Minimum matches played to qualify
            limit: Number of players to return
        """
        league_id_int = _ensure_int(league_id)
        if league_id_int is None:
            return []
        
        league_season_key = f"{league_id_int}_{season_id}"
        
        pipeline = [
            {"$match": {"league_season_key": league_season_key, "rating": {"$ne": None}}},
            {
                "$group": {
                    "_id": "$player_id",
                    "name": {"$first": "$name"},
                    "team_id": {"$last": "$team_id"},
                    "team_name": {"$last": "$team_name"},
                    "avg_rating": {"$avg": "$rating"},
                    "matches_played": {"$sum": 1},
                    "total_goals": {"$sum": "$goals"},
                    "total_assists": {"$sum": "$assists"}
                }
            },
            {"$match": {"matches_played": {"$gte": min_matches}}},
            {"$sort": {"avg_rating": -1}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "player_id": "$_id",
                    "name": 1,
                    "team_id": 1,
                    "team_name": 1,
                    "avg_rating": {"$round": ["$avg_rating", 2]},
                    "matches_played": 1,
                    "total_goals": 1,
                    "total_assists": 1
                }
            }
        ]
        
        return list(self.mongo.player_stats.aggregate(pipeline))
    
    def compare_players(
        self,
        player_ids: List[Union[int, str]],
        league_season_key: str = None
    ) -> List[dict]:
        """
        Compare multiple players' stats.
        
        Args:
            player_ids: List of player IDs to compare (int or string)
            league_season_key: Optional filter by league/season
            
        Returns:
            List of aggregated stats for each player
        """
        # Convert all player IDs to integers
        player_ids_int = [_ensure_int(pid) for pid in player_ids]
        player_ids_int = [pid for pid in player_ids_int if pid is not None]
        
        if not player_ids_int:
            return []
        
        query = {"player_id": {"$in": player_ids_int}}
        
        if league_season_key:
            query["league_season_key"] = league_season_key
        
        pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": "$player_id",
                    "name": {"$first": "$name"},
                    "team_name": {"$last": "$team_name"},
                    "matches_played": {"$sum": 1},
                    "total_goals": {"$sum": "$goals"},
                    "total_assists": {"$sum": "$assists"},
                    "total_minutes": {"$sum": "$minutes_played"},
                    "avg_rating": {"$avg": "$rating"},
                    "yellow_cards": {"$sum": {"$cond": ["$yellow_card", 1, 0]}},
                    "red_cards": {"$sum": {"$cond": ["$red_card", 1, 0]}}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "player_id": "$_id",
                    "name": 1,
                    "team_name": 1,
                    "matches_played": 1,
                    "total_goals": 1,
                    "total_assists": 1,
                    "total_minutes": 1,
                    "avg_rating": {"$round": ["$avg_rating", 2]},
                    "goals_per_90": {
                        "$round": [
                            {"$multiply": [{"$divide": ["$total_goals", {"$max": ["$total_minutes", 1]}]}, 90]},
                            2
                        ]
                    },
                    "assists_per_90": {
                        "$round": [
                            {"$multiply": [{"$divide": ["$total_assists", {"$max": ["$total_minutes", 1]}]}, 90]},
                            2
                        ]
                    },
                    "yellow_cards": 1,
                    "red_cards": 1
                }
            }
        ]
        
        return list(self.mongo.player_stats.aggregate(pipeline))
    
    def get_player_season_summary(
        self,
        player_id: Union[int, str],
        league_season_key: str
    ) -> Optional[dict]:
        """
        Get a player's season summary stats.
        
        Args:
            player_id: Player ID (int or string)
            league_season_key: League and season key
            
        Returns:
            Aggregated season stats for the player
        """
        player_id_int = _ensure_int(player_id)
        if player_id_int is None:
            return None
        
        pipeline = [
            {
                "$match": {
                    "player_id": player_id_int,
                    "league_season_key": league_season_key
                }
            },
            {
                "$group": {
                    "_id": "$player_id",
                    "name": {"$first": "$name"},
                    "team_name": {"$last": "$team_name"},
                    "matches_played": {"$sum": 1},
                    "total_goals": {"$sum": "$goals"},
                    "total_assists": {"$sum": "$assists"},
                    "total_minutes": {"$sum": "$minutes_played"},
                    "avg_rating": {"$avg": "$rating"},
                    "clean_sheets": {"$sum": {"$cond": [{"$eq": ["$is_goalkeeper", True]}, 1, 0]}},
                    "yellow_cards": {"$sum": {"$cond": ["$yellow_card", 1, 0]}},
                    "red_cards": {"$sum": {"$cond": ["$red_card", 1, 0]}}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "player_id": "$_id",
                    "name": 1,
                    "team_name": 1,
                    "matches_played": 1,
                    "total_goals": 1,
                    "total_assists": 1,
                    "total_minutes": 1,
                    "avg_rating": {"$round": ["$avg_rating", 2]},
                    "yellow_cards": 1,
                    "red_cards": 1
                }
            }
        ]
        
        results = list(self.mongo.player_stats.aggregate(pipeline))
        return results[0] if results else None
    
    # =========================================================================
    # Team Queries
    # =========================================================================
    
    def get_team_by_id(self, team_id: Union[int, str]) -> Optional[dict]:
        """Get a team by ID."""
        team_id_int = _ensure_int(team_id)
        if team_id_int is None:
            return None
        
        return self.mongo.teams.find_one(
            {"team_id": team_id_int},
            {"_id": 0}
        )
    
    def get_team_players(
        self,
        team_id: Union[int, str],
        league_season_key: str
    ) -> List[dict]:
        """
        Get all players for a team in a specific season.
        
        Returns aggregated stats for each player.
        """
        team_id_int = _ensure_int(team_id)
        if team_id_int is None:
            return []
        
        pipeline = [
            {
                "$match": {
                    "team_id": team_id_int,
                    "league_season_key": league_season_key
                }
            },
            {
                "$group": {
                    "_id": "$player_id",
                    "name": {"$first": "$name"},
                    "is_goalkeeper": {"$first": "$is_goalkeeper"},
                    "matches_played": {"$sum": 1},
                    "total_goals": {"$sum": "$goals"},
                    "total_assists": {"$sum": "$assists"},
                    "total_minutes": {"$sum": "$minutes_played"},
                    "avg_rating": {"$avg": "$rating"}
                }
            },
            {"$sort": {"total_minutes": -1}},
            {
                "$project": {
                    "_id": 0,
                    "player_id": "$_id",
                    "name": 1,
                    "is_goalkeeper": 1,
                    "matches_played": 1,
                    "total_goals": 1,
                    "total_assists": 1,
                    "total_minutes": 1,
                    "avg_rating": {"$round": ["$avg_rating", 2]}
                }
            }
        ]
        
        return list(self.mongo.player_stats.aggregate(pipeline))
    
    def get_team_season_stats(
        self,
        team_id: Union[int, str],
        league_season_key: str
    ) -> dict:
        """
        Get aggregated team stats for a season.
        """
        team_id_int = _ensure_int(team_id)
        if team_id_int is None:
            return {}
        
        # Get all matches for the team
        matches = list(self.mongo.matches.find(
            {
                "$or": [
                    {"home_team.team_id": team_id_int},
                    {"away_team.team_id": team_id_int}
                ],
                "league_season_key": league_season_key,
                "finished": True
            },
            {"home_team": 1, "away_team": 1, "stats_summary": 1}
        ))
        
        stats = {
            "matches_played": len(matches),
            "wins": 0,
            "draws": 0,
            "losses": 0,
            "goals_for": 0,
            "goals_against": 0
        }
        
        for match in matches:
            home_team = match.get("home_team", {})
            away_team = match.get("away_team", {})
            
            is_home = home_team.get("team_id") == team_id_int
            
            home_score = home_team.get("score", 0) or 0
            away_score = away_team.get("score", 0) or 0
            
            if is_home:
                stats["goals_for"] += home_score
                stats["goals_against"] += away_score
                if home_score > away_score:
                    stats["wins"] += 1
                elif home_score < away_score:
                    stats["losses"] += 1
                else:
                    stats["draws"] += 1
            else:
                stats["goals_for"] += away_score
                stats["goals_against"] += home_score
                if away_score > home_score:
                    stats["wins"] += 1
                elif away_score < home_score:
                    stats["losses"] += 1
                else:
                    stats["draws"] += 1
        
        stats["points"] = stats["wins"] * 3 + stats["draws"]
        stats["goal_difference"] = stats["goals_for"] - stats["goals_against"]
        
        return stats
    
    # =========================================================================
    # Search Queries
    # =========================================================================
    
    def search_players(self, query: str, limit: int = 20) -> List[dict]:
        """
        Search players by name.
        
        Args:
            query: Search query string
            limit: Maximum results
            
        Returns:
            List of matching players
        """
        return list(self.mongo.player_stats.find(
            {"$text": {"$search": query}},
            {"_id": 0, "score": {"$meta": "textScore"}}
        ).sort([("score", {"$meta": "textScore"})]).limit(limit))
    
    def search_teams(self, query: str, limit: int = 20) -> List[dict]:
        """Search teams by name."""
        return list(self.mongo.teams.find(
            {"$text": {"$search": query}},
            {"_id": 0}
        ).limit(limit))
    
    # =========================================================================
    # Player Profile Queries
    # =========================================================================
    
    def get_player_profile(self, player_id: Union[int, str]) -> Optional[dict]:
        """
        Get player profile from players collection.
        """
        player_id_int = _ensure_int(player_id)
        if player_id_int is None:
            return None
        
        return self.mongo.players.find_one(
            {"player_id": player_id_int},
            {"_id": 0}
        )
    
    def get_player_career_stats(self, player_id: Union[int, str]) -> dict:
        """
        Get aggregated career stats for a player across all seasons.
        """
        player_id_int = _ensure_int(player_id)
        if player_id_int is None:
            return {}
        
        pipeline = [
            {"$match": {"player_id": player_id_int}},
            {
                "$group": {
                    "_id": "$player_id",
                    "name": {"$first": "$name"},
                    "total_matches": {"$sum": 1},
                    "total_goals": {"$sum": "$goals"},
                    "total_assists": {"$sum": "$assists"},
                    "total_minutes": {"$sum": "$minutes_played"},
                    "avg_rating": {"$avg": "$rating"},
                    "seasons_played": {"$addToSet": "$league_season_key"},
                    "teams_played_for": {"$addToSet": "$team_name"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "player_id": "$_id",
                    "name": 1,
                    "total_matches": 1,
                    "total_goals": 1,
                    "total_assists": 1,
                    "total_minutes": 1,
                    "avg_rating": {"$round": ["$avg_rating", 2]},
                    "seasons_count": {"$size": "$seasons_played"},
                    "teams_played_for": 1
                }
            }
        ]
        
        results = list(self.mongo.player_stats.aggregate(pipeline))
        return results[0] if results else {}


# =============================================================================
# Convenience Functions
# =============================================================================

def get_query_helpers(mongo_service: MongoDBService = None) -> QueryHelpers:
    """Get QueryHelpers instance."""
    return QueryHelpers(mongo_service)
