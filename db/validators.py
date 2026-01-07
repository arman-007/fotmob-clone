"""
Data Validators for Football Stats Platform

Pydantic models for validating data before MongoDB insertion.
Ensures data integrity and prevents malformed data from entering the database.

Updated: Integer IDs version
- league_id: int
- match_id: int
- player_id: int  
- team_id: int
- Removed player_match_key (replaced by compound unique index)
"""

from datetime import datetime
from typing import Optional, List, Any, Union
from pydantic import BaseModel, Field, field_validator, model_validator
import re


# =============================================================================
# Helper Functions
# =============================================================================

def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely convert a value to integer."""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return default
        try:
            return int(value)
        except ValueError:
            return default
    if isinstance(value, float):
        return int(value)
    return default


# =============================================================================
# League Validators
# =============================================================================

class LeagueValidator(BaseModel):
    """Validates league data before insertion."""
    
    league_id: int = Field(..., ge=1)
    name: str = Field(..., min_length=1, max_length=200)
    localized_name: Optional[str] = Field(default=None, max_length=200)
    country_code: Optional[str] = Field(default="", max_length=10)
    page_url: Optional[str] = Field(default="", max_length=500)
    category: Optional[str] = Field(default="domestic")
    
    @field_validator('league_id', mode='before')
    @classmethod
    def convert_league_id_to_int(cls, v):
        if v is None:
            raise ValueError("league_id cannot be None")
        result = safe_int(v)
        if result is None:
            raise ValueError(f"league_id must be convertible to int, got: {v}")
        return result
    
    @field_validator('name', mode='before')
    @classmethod
    def validate_name(cls, v):
        if not v or not str(v).strip():
            raise ValueError("name cannot be empty")
        return str(v).strip()
    
    @field_validator('country_code', mode='before')
    @classmethod
    def validate_country_code(cls, v):
        if v is None:
            return ""
        return str(v).strip().upper()


class SeasonInfoValidator(BaseModel):
    """Validates season reference in league document."""
    
    season_id: str = Field(..., min_length=4, max_length=20)
    is_current: bool = Field(default=False)


# =============================================================================
# Season Validators
# =============================================================================

class SeasonValidator(BaseModel):
    """Validates season data before insertion."""
    
    league_id: int = Field(..., ge=1)
    season_id: str = Field(..., min_length=4, max_length=20)
    league_season_key: Optional[str] = None
    league_name: Optional[str] = Field(default="", max_length=200)
    country_code: Optional[str] = Field(default="", max_length=10)
    all_available_seasons: Optional[List[str]] = Field(default_factory=list)
    
    @model_validator(mode='after')
    def set_league_season_key(self):
        if not self.league_season_key:
            self.league_season_key = f"{self.league_id}_{self.season_id}"
        return self
    
    @field_validator('league_id', mode='before')
    @classmethod
    def convert_league_id(cls, v):
        if v is None:
            raise ValueError("league_id cannot be None")
        result = safe_int(v)
        if result is None:
            raise ValueError(f"league_id must be convertible to int, got: {v}")
        return result
    
    @field_validator('season_id', mode='before')
    @classmethod
    def convert_season_id(cls, v):
        if v is None:
            raise ValueError("season_id cannot be None")
        return str(v).strip()
    
    @field_validator('season_id', mode='after')
    @classmethod
    def validate_season_format(cls, v):
        # Allow formats like: 2024-25, 2024-2025, 2024/25, 2024/2025, 2024
        pattern = r'^(\d{4}[-/]?\d{0,4})$'
        if not re.match(pattern, v):
            raise ValueError(f"Invalid season format: {v}. Expected format like '2024-25', '2024-2025', or '2024'")
        return v


# =============================================================================
# Team Validators
# =============================================================================

class TeamValidator(BaseModel):
    """Validates team data before insertion."""
    
    team_id: int = Field(..., ge=1)
    name: str = Field(..., min_length=1, max_length=200)
    
    @field_validator('team_id', mode='before')
    @classmethod
    def convert_team_id_to_int(cls, v):
        if v is None:
            raise ValueError("team_id cannot be None")
        result = safe_int(v)
        if result is None:
            raise ValueError(f"team_id must be convertible to int, got: {v}")
        return result
    
    @field_validator('name', mode='before')
    @classmethod
    def validate_name(cls, v):
        if not v:
            return "Unknown Team"
        return str(v).strip()


class TeamEmbeddedValidator(BaseModel):
    """Validates embedded team data in match documents."""
    
    team_id: Optional[int] = Field(default=None, ge=1)
    name: str = Field(default="Unknown Team", max_length=200)
    score: Optional[int] = Field(default=None, ge=0, le=50)
    
    @field_validator('team_id', mode='before')
    @classmethod
    def convert_to_int(cls, v):
        if v is None:
            return None
        return safe_int(v)


# =============================================================================
# Player Stats Validators
# =============================================================================

class PlayerStatValidator(BaseModel):
    """Validates individual player statistics (for embedded use in matches)."""
    
    player_id: int = Field(..., ge=1)
    name: str = Field(default="Unknown Player", max_length=200)
    team_id: Optional[int] = Field(default=None, ge=1)
    team_name: str = Field(default="", max_length=200)
    is_goalkeeper: bool = Field(default=False)
    
    # Core stats
    goals: int = Field(default=0, ge=0, le=20)
    assists: int = Field(default=0, ge=0, le=20)
    yellow_card: bool = Field(default=False)
    red_card: bool = Field(default=False)
    rating: Optional[float] = Field(default=None, ge=0, le=10)
    minutes_played: int = Field(default=0, ge=0, le=150)
    
    # Additional stats stored dynamically
    additional_stats: dict = Field(default_factory=dict)
    
    @field_validator('player_id', mode='before')
    @classmethod
    def convert_player_id(cls, v):
        if v is None:
            raise ValueError("player_id cannot be None")
        result = safe_int(v)
        if result is None:
            raise ValueError(f"player_id must be convertible to int, got: {v}")
        return result
    
    @field_validator('team_id', mode='before')
    @classmethod
    def convert_team_id(cls, v):
        if v is None:
            return None
        return safe_int(v)
    
    @field_validator('goals', 'assists', 'minutes_played', mode='before')
    @classmethod
    def convert_to_int(cls, v):
        if v is None:
            return 0
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0
    
    @field_validator('rating', mode='before')
    @classmethod
    def convert_rating(cls, v):
        if v is None:
            return None
        try:
            rating = float(v)
            if rating < 0 or rating > 10:
                return None
            return round(rating, 2)
        except (ValueError, TypeError):
            return None
    
    @field_validator('yellow_card', 'red_card', 'is_goalkeeper', mode='before')
    @classmethod
    def convert_to_bool(cls, v):
        if v is None:
            return False
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.lower() in ('true', '1', 'yes')
        return bool(v)


class PlayerMatchStatValidator(BaseModel):
    """
    Validates flattened player-match stat record for player_stats collection.
    
    Note: player_match_key is removed - using compound unique index on (player_id, match_id) instead.
    """
    
    player_id: int = Field(..., ge=1)
    name: str = Field(default="Unknown Player", max_length=200)
    team_id: Optional[int] = Field(default=None, ge=1)
    team_name: str = Field(default="", max_length=200)
    is_goalkeeper: bool = Field(default=False)
    
    # Match context
    match_id: int = Field(..., ge=1)
    match_datetime_utc: Optional[datetime] = None
    league_id: Optional[int] = Field(default=None, ge=1)
    season_id: str = Field(default="", max_length=20)
    league_season_key: str = Field(default="", max_length=50)
    
    # Opponent info
    opponent_team_id: Optional[int] = Field(default=None, ge=1)
    opponent_team_name: str = Field(default="", max_length=200)
    is_home: bool = Field(default=False)
    
    # Stats
    goals: int = Field(default=0, ge=0, le=20)
    assists: int = Field(default=0, ge=0, le=20)
    yellow_card: bool = Field(default=False)
    red_card: bool = Field(default=False)
    rating: Optional[float] = Field(default=None, ge=0, le=10)
    minutes_played: int = Field(default=0, ge=0, le=150)
    
    @field_validator('player_id', 'match_id', mode='before')
    @classmethod
    def convert_required_ids(cls, v):
        if v is None:
            raise ValueError("Value cannot be None")
        result = safe_int(v)
        if result is None:
            raise ValueError(f"Value must be convertible to int, got: {v}")
        return result
    
    @field_validator('team_id', 'league_id', 'opponent_team_id', mode='before')
    @classmethod
    def convert_optional_ids(cls, v):
        if v is None:
            return None
        return safe_int(v)
    
    @field_validator('season_id', mode='before')
    @classmethod
    def convert_season_id(cls, v):
        if v is None:
            return ""
        return str(v).strip()


# =============================================================================
# Match Validators
# =============================================================================

class MatchStatsSummaryValidator(BaseModel):
    """Validates match statistics summary."""
    
    total_goals: int = Field(default=0, ge=0, le=50)
    total_yellow_cards: int = Field(default=0, ge=0, le=30)
    total_red_cards: int = Field(default=0, ge=0, le=10)


class MatchValidator(BaseModel):
    """Validates match data before insertion."""
    
    match_id: int = Field(..., ge=1)
    league_id: Optional[int] = Field(default=None, ge=1)
    season_id: str = Field(default="", max_length=20)
    league_season_key: Optional[str] = None
    match_name: str = Field(default="", max_length=300)
    match_datetime_utc: Optional[datetime] = None
    started: bool = Field(default=False)
    finished: bool = Field(default=False)
    home_team: Optional[TeamEmbeddedValidator] = None
    away_team: Optional[TeamEmbeddedValidator] = None
    player_stats: List[dict] = Field(default_factory=list)
    stats_summary: Optional[MatchStatsSummaryValidator] = None
    
    @model_validator(mode='after')
    def set_league_season_key(self):
        if not self.league_season_key and self.league_id and self.season_id:
            self.league_season_key = f"{self.league_id}_{self.season_id}"
        return self
    
    @field_validator('match_id', mode='before')
    @classmethod
    def convert_match_id(cls, v):
        if v is None:
            raise ValueError("match_id cannot be None")
        result = safe_int(v)
        if result is None:
            raise ValueError(f"match_id must be convertible to int, got: {v}")
        return result
    
    @field_validator('league_id', mode='before')
    @classmethod
    def convert_league_id(cls, v):
        if v is None:
            return None
        return safe_int(v)
    
    @field_validator('season_id', mode='before')
    @classmethod
    def convert_season_id(cls, v):
        if v is None:
            return ""
        return str(v).strip()


# =============================================================================
# Player Profile Validators (for players collection)
# =============================================================================

class PlayerSeasonSummaryValidator(BaseModel):
    """Validates player season summary in player profiles."""
    
    league_id: Optional[int] = Field(default=None, ge=1)
    season_id: str = Field(default="", max_length=20)
    league_season_key: str = Field(default="", max_length=50)
    team_id: Optional[int] = Field(default=None, ge=1)
    team_name: str = Field(default="", max_length=200)
    matches: int = Field(default=0, ge=0)
    goals: int = Field(default=0, ge=0)
    assists: int = Field(default=0, ge=0)
    minutes_played: int = Field(default=0, ge=0)
    avg_rating: Optional[float] = Field(default=None, ge=0, le=10)
    
    @field_validator('league_id', 'team_id', mode='before')
    @classmethod
    def convert_ids(cls, v):
        if v is None:
            return None
        return safe_int(v)


class PlayerProfileValidator(BaseModel):
    """Validates aggregated player profile data."""
    
    player_id: int = Field(..., ge=1)
    name: str = Field(default="Unknown Player", max_length=200)
    current_team_id: Optional[int] = Field(default=None, ge=1)
    current_team_name: Optional[str] = Field(default=None, max_length=200)
    is_goalkeeper: bool = Field(default=False)
    total_matches: int = Field(default=0, ge=0)
    total_goals: int = Field(default=0, ge=0)
    total_assists: int = Field(default=0, ge=0)
    total_minutes: int = Field(default=0, ge=0)
    avg_rating: Optional[float] = Field(default=None, ge=0, le=10)
    seasons: List[PlayerSeasonSummaryValidator] = Field(default_factory=list)
    
    @field_validator('player_id', mode='before')
    @classmethod
    def convert_player_id(cls, v):
        if v is None:
            raise ValueError("player_id cannot be None")
        result = safe_int(v)
        if result is None:
            raise ValueError(f"player_id must be convertible to int, got: {v}")
        return result
    
    @field_validator('current_team_id', mode='before')
    @classmethod
    def convert_team_id(cls, v):
        if v is None:
            return None
        return safe_int(v)


# =============================================================================
# Validation Result Classes
# =============================================================================

class ValidationResult(BaseModel):
    """Result of a validation operation."""
    
    is_valid: bool
    data: Optional[dict] = None
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


# =============================================================================
# Validation Helper Functions
# =============================================================================

def validate_league(data: dict) -> ValidationResult:
    """Validate league data and return result."""
    try:
        validated = LeagueValidator(**data)
        return ValidationResult(is_valid=True, data=validated.model_dump())
    except Exception as e:
        return ValidationResult(is_valid=False, errors=[str(e)])


def validate_season(data: dict) -> ValidationResult:
    """Validate season data and return result."""
    try:
        validated = SeasonValidator(**data)
        return ValidationResult(is_valid=True, data=validated.model_dump())
    except Exception as e:
        return ValidationResult(is_valid=False, errors=[str(e)])


def validate_match(data: dict) -> ValidationResult:
    """Validate match data and return result."""
    try:
        validated = MatchValidator(**data)
        return ValidationResult(is_valid=True, data=validated.model_dump())
    except Exception as e:
        return ValidationResult(is_valid=False, errors=[str(e)])


def validate_player_stat(data: dict) -> ValidationResult:
    """Validate player stat data and return result."""
    try:
        validated = PlayerStatValidator(**data)
        return ValidationResult(is_valid=True, data=validated.model_dump())
    except Exception as e:
        return ValidationResult(is_valid=False, errors=[str(e)])


def validate_player_match_stat(data: dict) -> ValidationResult:
    """Validate player match stat data and return result."""
    try:
        validated = PlayerMatchStatValidator(**data)
        return ValidationResult(is_valid=True, data=validated.model_dump())
    except Exception as e:
        return ValidationResult(is_valid=False, errors=[str(e)])


def validate_team(data: dict) -> ValidationResult:
    """Validate team data and return result."""
    try:
        validated = TeamValidator(**data)
        return ValidationResult(is_valid=True, data=validated.model_dump())
    except Exception as e:
        return ValidationResult(is_valid=False, errors=[str(e)])
