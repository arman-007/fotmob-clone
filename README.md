# ⚽ Football Stats Pipeline

A robust, production-ready data pipeline for fetching and storing comprehensive football (soccer) statistics from FotMob API. Features both **historical data ingestion** and **daily incremental updates**, designed for Fantasy Premier League-style applications with MongoDB integration.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![MongoDB](https://img.shields.io/badge/MongoDB-6.0+-green.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Historical Pipeline](#historical-pipeline)
  - [Daily Pipeline](#daily-pipeline)
- [Database Schema](#database-schema)
- [Query Helpers API](#query-helpers-api)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

The Football Stats Pipeline is a comprehensive data ingestion system with **two complementary pipelines**:

| Pipeline | Purpose | Use Case |
|----------|---------|----------|
| **Historical Pipeline** (`pipeline.py`) | Bulk fetch historical data | Initial database population, backfilling seasons |
| **Daily Pipeline** (`daily_pipeline.py`) | Incremental daily updates | Keeping data current, daily cron jobs |

Both pipelines share the same MongoDB schema and modules, ensuring data consistency.

### Key Design Principles

- **Integer IDs**: All IDs (league, match, player, team) are stored as **integers** for consistency and query performance
- **In-Memory Data Flow**: Data is passed in-memory between functions - no file reads required
- **True `--no-json`**: When disabled, absolutely no JSON files are created
- **MongoDB First**: MongoDB is the primary data store; JSON is optional for debugging

### Use Cases

- Building Fantasy Football applications
- Sports analytics dashboards
- Player performance tracking systems
- Historical football data analysis
- Machine learning datasets for sports predictions
- Live match day statistics

---

## ✨ Features

### Core Features

| Feature | Description |
|---------|-------------|
| **Multi-League Support** | Fetches data from 500+ leagues worldwide |
| **League Source Selection** | Choose popular, international, countries, or all leagues |
| **Historical Data** | Retrieves up to 10 seasons per league |
| **Daily Updates** | Incremental updates for current matches |
| **Player Statistics** | Comprehensive stats including goals, assists, ratings, cards |
| **Match Details** | Full match information with player-level granularity |
| **Team Data** | Team profiles with aggregated statistics |

### Pipeline Features

| Feature | Historical | Daily |
|---------|:----------:|:-----:|
| Checkpoint/Resume | ✅ | - |
| Failed Match Retry | ✅ | - |
| League Source Selection | ✅ | - |
| League Filtering | ✅ | ✅ |
| Skip Specific Leagues | ✅ | - |
| Date Selection | - | ✅ |
| Match Status Filtering | - | ✅ |
| Dry Run Mode | - | ✅ |
| Safe Updates | - | ✅ |
| Progress Tracking | ✅ | ✅ |
| Optional JSON Output | ✅ | ✅ |

### Query Features

| Feature | Description |
|---------|-------------|
| **Top Scorers** | Aggregated goal statistics by league/season |
| **Top Assists** | Aggregated assist statistics |
| **Player Ratings** | Average ratings with minimum match filters |
| **Player Comparison** | Compare multiple players side-by-side |
| **Team Statistics** | Aggregated team performance data |
| **Search** | Text search for players and teams |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DUAL PIPELINE SYSTEM                        │
└─────────────────────────────────────────────────────────────────┘

                    ┌──────────────────┐
                    │   FotMob API     │
                    └────────┬─────────┘
                             │
            ┌────────────────┴────────────────┐
            │                                 │
            ▼                                 ▼
   ┌─────────────────┐               ┌─────────────────┐
   │   Historical    │               │     Daily       │
   │   Pipeline      │               │    Pipeline     │
   │  (pipeline.py)  │               │(daily_pipeline) │
   └────────┬────────┘               └────────┬────────┘
            │                                 │
            │    ┌─────────────────────┐      │
            └───►│  Shared Modules     │◄─────┘
                 │  - mongodb_service  │
                 │  - validators       │
                 │  - match_processor  │
                 └──────────┬──────────┘
                            │
               ┌────────────┴────────────┐
               ▼                         ▼
        ┌──────────────┐          ┌──────────────┐
        │   MongoDB    │          │  JSON Files  │
        │  (Primary)   │          │  (Optional)  │
        └──────────────┘          └──────────────┘
```

### In-Memory Data Flow

The pipeline uses an **in-memory data flow** pattern - no intermediate file reads required:

```
┌─────────────────────────────────────────────────────────────────┐
│                    IN-MEMORY DATA FLOW                          │
└─────────────────────────────────────────────────────────────────┘

1. Fetch Leagues (API)
        │
        ▼
   ┌─────────────┐
   │ In-Memory   │──────► MongoDB (always)
   │ leagues_data│──────► JSON (optional)
   └─────────────┘
        │
        ▼ Returns league IDs directly (no file read)

2. Fetch Season Data (API)
        │
        ▼
   ┌─────────────┐
   │ In-Memory   │──────► MongoDB (always)
   │ season_data │──────► JSON (optional)
   │ + match_ids │
   └─────────────┘
        │
        ▼ Returns match IDs directly (no file read)

3. Process Matches (API)
        │
        ▼
   ┌─────────────┐
   │ In-Memory   │──────► MongoDB (always)
   │ match_stats │──────► JSON (optional)
   └─────────────┘
```

This means `--no-json` **truly** creates zero JSON files.

---

## 📦 Prerequisites

### Required

- **Python 3.9+**
- **MongoDB 6.0+** (local or cloud)
- **Chrome/Chromium** (for Selenium)
- **ChromeDriver** (matching your Chrome version)

### Python Dependencies

```txt
pymongo>=4.6.0
pydantic>=2.0.0
requests>=2.31.0
python-dotenv>=1.0.0
selenium-wire>=5.1.0
selenium>=4.15.0
tzlocal>=5.2
brotli>=1.1.0
```

---

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/football-stats-pipeline.git
cd football-stats-pipeline
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Install ChromeDriver

```bash
# Ubuntu/Debian
sudo apt-get install chromium-chromedriver

# Mac (with Homebrew)
brew install chromedriver

# Or download from: https://chromedriver.chromium.org/
```

### 5. Set Up MongoDB

```bash
# Start MongoDB locally
sudo systemctl start mongod

# Or use Docker
docker run -d -p 27017:27017 --name mongodb mongo:latest
```

### 6. Create Environment File

```bash
cp .env.example .env
# Edit .env with your configuration
```

---

## ⚙️ Configuration

### Environment Variables (`.env`)

```env
# API Configuration
URL=https://www.fotmob.com/api

# Locale Settings
TIMEZONE=UTC
CCODE3=BGD

# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=football_stats
```

### Configuration Options

| Variable | Description | Default |
|----------|-------------|---------|
| `URL` | FotMob API base URL | `https://www.fotmob.com/api` |
| `TIMEZONE` | Default timezone | `UTC` |
| `CCODE3` | Country code for locale | `BGD` |
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017` |
| `MONGODB_DATABASE` | Database name | `football_stats` |

> ⚠️ **Important**: Use `https://www.fotmob.com/api` (NOT `https://www.fotmob.com/api/data`)

---

## 📖 Usage

### Historical Pipeline

The historical pipeline (`pipeline.py`) is designed for **bulk data ingestion** - fetching multiple seasons of historical data across many leagues.

#### Basic Commands

```bash
# Run pipeline - process ALL leagues (default)
python pipeline.py

# Process only popular leagues
python pipeline.py --source popular

# Process all country leagues
python pipeline.py --source countries

# Process international competitions only
python pipeline.py --source international

# Check pipeline progress
python pipeline.py --status

# Skip JSON files (faster, production mode)
python pipeline.py --no-json

# Process limited leagues (for testing)
python pipeline.py --league-limit 2

# Skip specific problematic leagues
python pipeline.py --skip-leagues 10913,285,9173

# Force re-process all data (ignore checkpoints)
python pipeline.py --force

# Retry only previously failed matches
python pipeline.py --retry-failed

# Build aggregated player profiles after ingestion
python pipeline.py --build-players
```

#### CLI Options Reference

| Flag | Description | Default |
|------|-------------|---------|
| `--source` | League source: `popular`, `international`, `countries`, `all` | `all` |
| `--no-json` | Skip saving JSON files (faster) | `False` |
| `--no-mongodb` | Skip MongoDB, JSON only (debugging) | `False` |
| `--league-limit N` | Process only first N leagues | All leagues |
| `--skip-leagues` | Comma-separated league IDs to skip | None |
| `--force` | Ignore checkpoints, re-process all | `False` |
| `--retry-failed` | Only retry previously failed matches | `False` |
| `--status` | Show progress status and exit | - |
| `--build-players` | Build aggregated player profiles | `False` |
| `-d, --date` | Date parameter (YYYYMMDD) | Today |

#### League Source Options

| Source | Description | Typical Count |
|--------|-------------|---------------|
| `popular` | Top leagues (Premier League, La Liga, etc.) | ~20 leagues |
| `international` | International competitions (World Cup, Euros, etc.) | ~30 leagues |
| `countries` | All domestic leagues from all countries | ~500 leagues |
| `all` | Everything combined | ~550 leagues |

#### Example Workflows

```bash
# Initial setup: Full historical data load (all leagues)
python pipeline.py --no-json

# Quick test: Just popular leagues, 2 only
python pipeline.py --source popular --league-limit 2

# Production: All country leagues, no JSON
python pipeline.py --source countries --no-json

# International tournaments only
python pipeline.py --source international

# Skip problematic leagues that cause errors
python pipeline.py --skip-leagues 10913,285,9173,10175

# Resume after interruption (automatic)
python pipeline.py

# Full re-run with player profiles
python pipeline.py --force --build-players

# Check progress
python pipeline.py --status
```

#### Status Output Example

```
============================================================
PIPELINE PROGRESS STATUS
============================================================

📊 Overall Progress:
   Total Seasons Tracked: 45
   Total Matches: 15,200
   Processed Matches: 12,450
   Failed Matches: 23

📋 By Status:
   ✅ completed: 38 seasons
   🔄 in_progress: 2 seasons
   ⚠️ partially_completed: 3 seasons
   ❌ failed: 2 seasons

============================================================
```

---

### Daily Pipeline

The daily pipeline (`daily_pipeline.py`) is designed for **incremental updates** - fetching matches for a specific date to keep your database current.

#### Basic Commands

```bash
# Fetch today's matches
python daily_pipeline.py

# Fetch matches for a specific date
python daily_pipeline.py -d 20241215

# Filter by specific leagues (Premier League, La Liga, Bundesliga)
python daily_pipeline.py --leagues 47,87,54

# Only process finished matches
python daily_pipeline.py --finished-only

# Preview what would be processed (no changes)
python daily_pipeline.py --dry-run

# Show match summary for a date
python daily_pipeline.py --status

# Skip JSON output (faster)
python daily_pipeline.py --no-json

# Test with limited matches
python daily_pipeline.py --match-limit 10 -v
```

#### CLI Options Reference

| Flag | Description | Default |
|------|-------------|---------|
| `-d, --date` | Date to fetch (YYYYMMDD format) | Today |
| `--leagues` | Comma-separated league IDs to filter | All leagues |
| `--no-json` | Skip saving JSON files | `False` |
| `--no-mongodb` | Skip saving to MongoDB | `False` |
| `--output-dir` | Output directory for JSON files | `output/daily` |
| `--finished-only` | Only process finished matches | `False` |
| `--started-only` | Only process started/in-progress matches | `False` |
| `--match-limit N` | Limit number of matches (for testing) | No limit |
| `--dry-run` | Preview without processing | `False` |
| `--status` | Show match summary and exit | - |
| `--force` | Bypass safety checks, force update | `False` |
| `-v, --verbose` | Enable verbose/debug logging | `False` |

#### Flag Details

##### `--leagues`
Filter matches by specific league IDs (integers). Useful for focusing on specific competitions.

```bash
# Premier League only
python daily_pipeline.py --leagues 47

# Multiple leagues
python daily_pipeline.py --leagues 47,87,54,53

# Common league IDs:
#   47  = Premier League (England)
#   87  = La Liga (Spain)
#   54  = Bundesliga (Germany)
#   53  = Serie A (Italy)
#   55  = Ligue 1 (France)
#   42  = Champions League
```

##### `--dry-run`
Preview what would be processed without making any changes. Great for testing filters.

```bash
python daily_pipeline.py -d 20241215 --finished-only --dry-run
```

Output:
```
🔍 DRY RUN - Would process the following matches:
  ✅ 4813566 - Premier League
  ✅ 4813567 - La Liga
  ✅ 4813568 - Serie A
  ... and 35 more
```

##### `--status`
Display match summary for the date without processing anything.

```bash
python daily_pipeline.py -d 20241215 --status
```

Output:
```
============================================================
DAILY MATCH STATUS FOR 20241215
============================================================

📊 Summary:
   Total Leagues: 45
   Total Matches: 128

📈 By Status:
   ⏳ Not Started: 23
   🔄 In Progress: 5
   ✅ Finished: 100

🏆 Top Leagues (by match count):
   Premier League: 10 matches
   La Liga: 10 matches
   Serie A: 10 matches
   ...
============================================================
```

#### Example Workflows

```bash
# Morning cron job: Yesterday's completed matches
python daily_pipeline.py -d $(date -d "yesterday" +%Y%m%d) --finished-only --no-json

# Live match day: All started matches
python daily_pipeline.py --started-only

# End of day: All finished matches for today
python daily_pipeline.py --finished-only

# Specific league focus
python daily_pipeline.py --leagues 47 --finished-only

# Testing new setup
python daily_pipeline.py --match-limit 5 --dry-run -v
```

#### Cron Job Setup

```bash
# Add to crontab for automatic daily updates
crontab -e

# Run at 6 AM daily - fetch yesterday's finished matches
0 6 * * * cd /path/to/football-stats-pipeline && /path/to/venv/bin/python daily_pipeline.py -d $(date -d "yesterday" +\%Y\%m\%d) --finished-only --no-json >> logs/cron.log 2>&1

# Run every 2 hours during match days (weekends)
0 */2 * * 0,6 cd /path/to/football-stats-pipeline && /path/to/venv/bin/python daily_pipeline.py --finished-only --no-json >> logs/cron.log 2>&1
```

---

## 🗄️ Database Schema

### ID Types

> **Important**: All IDs are stored as **integers** for consistency and query performance.

| Field | Type | Example |
|-------|------|---------|
| `league_id` | `int` | `47` |
| `match_id` | `int` | `4521342` |
| `player_id` | `int` | `961995` |
| `team_id` | `int` | `8650` |
| `season_id` | `string` | `"2024-2025"` |
| `league_season_key` | `string` | `"47_2024-2025"` |
| `player_match_key` | `string` | `"961995_4521342"` |

### Collections Overview

| Collection | Purpose | Key Fields |
|------------|---------|------------|
| `leagues` | League metadata | `league_id` (int), `name`, `country_code` |
| `seasons` | Season information | `league_season_key`, `league_id` (int), `season_id`, `match_ids` |
| `matches` | Match details with embedded player stats | `match_id` (int), `home_team`, `away_team`, `player_stats` |
| `player_stats` | Flattened per-match player stats | `player_match_key`, `player_id` (int), `match_id` (int) |
| `players` | Aggregated player profiles | `player_id` (int), `total_goals`, `total_assists` |
| `teams` | Team information | `team_id` (int), `name` |
| `pipeline_state` | Historical pipeline checkpoints | `league_id` (int), `season_id`, `status` |

### Sample Documents

#### player_stats
```javascript
{
  "player_match_key": "961995_4521342",
  "player_id": 961995,              // Integer
  "name": "Mohamed Salah",
  "team_id": 8650,                  // Integer
  "team_name": "Liverpool",
  "match_id": 4521342,              // Integer
  "match_datetime_utc": ISODate("2024-12-07T15:00:00Z"),
  "league_id": 47,                  // Integer
  "season_id": "2024-2025",
  "league_season_key": "47_2024-2025",
  "opponent_team_id": 8455,         // Integer
  "opponent_team_name": "Everton",
  "is_home": true,
  "goals": 2,
  "assists": 1,
  "yellow_card": false,
  "red_card": false,
  "rating": 9.2,
  "minutes_played": 90,
  "is_goalkeeper": false
}
```

#### matches
```javascript
{
  "match_id": 4521342,              // Integer
  "league_id": 47,                  // Integer
  "season_id": "2024-2025",
  "league_season_key": "47_2024-2025",
  "match_name": "Liverpool vs Everton",
  "match_datetime_utc": ISODate("2024-12-07T15:00:00Z"),
  "started": true,
  "finished": true,
  "home_team": {
    "team_id": 8650,                // Integer
    "name": "Liverpool"
  },
  "away_team": {
    "team_id": 8455,                // Integer
    "name": "Everton"
  },
  "player_stats": [...],            // Embedded array with integer IDs
  "stats_summary": {
    "total_goals": 3,
    "total_yellow_cards": 4,
    "total_red_cards": 0
  }
}
```

#### seasons
```javascript
{
  "league_id": 47,                  // Integer
  "season_id": "2024-2025",
  "league_season_key": "47_2024-2025",
  "league_name": "Premier League",
  "country_code": "ENG",
  "match_ids": [4521342, 4521343, 4521344, ...],  // Integer array
  "stats_summary": {
    "total_matches": 380,
    "completed_matches": 250
  }
}
```

### Indexes

Optimized indexes are automatically created for:

- Primary key lookups (`league_id`, `match_id`, `player_id`) - all integers
- Composite queries (`league_season_key` + `match_datetime_utc`)
- Aggregation pipelines (goals, assists, rating)
- Text search (player names, team names)

---

## 🔍 Query Helpers API

The `QueryHelpers` class provides ready-to-use methods for your application.

> **Note**: All ID parameters should be **integers**.

### Usage

```python
from db.query_helpers import QueryHelpers

queries = QueryHelpers()
```

### Available Methods

#### League Queries

```python
# Get all leagues
leagues = queries.get_all_leagues(category="popular")

# Get single league (integer ID)
league = queries.get_league_by_id(47)

# Get seasons for a league
seasons = queries.get_league_seasons(47)
```

#### Match Queries

```python
# Get matches for league/season
matches = queries.get_matches_for_league_season(47, "2024-2025", finished_only=True)

# Get single match with player stats (integer ID)
match = queries.get_match_by_id(4521342, include_player_stats=True)

# Get recent matches
recent = queries.get_recent_matches(league_id=47, days=7, limit=20)

# Get team matches (integer team ID)
team_matches = queries.get_team_matches(8650, league_season_key="47_2024-2025")
```

#### Player Stats Queries

```python
# Get player stats (integer player ID)
stats = queries.get_player_stats(961995, league_season_key="47_2024-2025")

# Get player form (last N matches)
form = queries.get_player_form(961995, matches=5)

# Get top scorers
top_scorers = queries.get_top_scorers(47, "2024-2025", limit=20)

# Get top assists
top_assists = queries.get_top_assists(47, "2024-2025", limit=20)

# Get top rated players
top_rated = queries.get_top_rated_players(47, "2024-2025", min_matches=5, limit=20)

# Compare players (integer IDs)
comparison = queries.compare_players(
    player_ids=[961995, 237079],
    league_season_key="47_2024-2025"
)

# Get player season summary
summary = queries.get_player_season_summary(961995, "47_2024-2025")
```

#### Team Queries

```python
# Get team by ID (integer)
team = queries.get_team_by_id(8650)

# Get team players with stats
players = queries.get_team_players(8650, "47_2024-2025")

# Get team season stats
team_stats = queries.get_team_season_stats(8650, "47_2024-2025")
```

#### Search Queries

```python
# Search players
players = queries.search_players("Salah", limit=20)

# Search teams
teams = queries.search_teams("Liverpool", limit=20)
```

---

## 📁 Project Structure

```
football-stats-pipeline/
│
├── pipeline.py               # Historical data pipeline
├── daily_pipeline.py         # Daily incremental updates pipeline
├── get_additional_stats.py   # Additional stats processor
├── requirements.txt          # Python dependencies
├── .env                      # Environment configuration
├── .env.example              # Example environment file
├── README.md                 # This file
│
├── service/                  # API interaction modules
│   ├── __init__.py
│   ├── get_auth_headers.py       # X-MAS token capture
│   ├── get_leagues.py            # League fetching (returns in-memory)
│   ├── get_specific_league.py    # Season data fetching (returns match IDs in-memory)
│   ├── get_player_stats.py       # Player stats (accepts league_id, season_id directly)
│   ├── get_daily_matches.py      # Daily match fetching
│   └── match_stats_processor.py  # Shared match processing
│
├── db/                       # Database modules
│   ├── __init__.py
│   ├── mongodb_service.py    # MongoDB operations
│   ├── validators.py         # Pydantic validators
│   ├── query_helpers.py      # Query helper functions
│   └── pipeline_state.py     # Checkpoint/resume system
│
├── utils/                    # Utility modules
│   ├── __init__.py
│   └── get_timezone.py       # Timezone utilities
│
├── logs/                     # Log files
│   ├── pipeline_log.txt      # Historical pipeline logs
│   └── daily_pipeline.log    # Daily pipeline logs
│
└── output/                   # JSON output (optional, only with --no-json disabled)
    ├── leagues_data.json     # All leagues
    ├── leagues/              # Historical data
    │   └── {league_id}/
    │       └── {season}/
    │           ├── league_info_*.json
    │           └── player_stats/
    │               └── player_stats_matchID_*.json
    └── daily/                # Daily data
        └── matches_{date}.json
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. 403 Forbidden Errors

**Cause**: Invalid or expired X-MAS token

**Solution**:
```bash
# Check your .env URL (should NOT have /data)
URL=https://www.fotmob.com/api  # ✅ Correct
URL=https://www.fotmob.com/api/data  # ❌ Wrong

# Ensure Chrome/Chromium is installed
which chromium-browser
```

#### 2. MongoDB Connection Failed

**Cause**: MongoDB not running or wrong URI

**Solution**:
```bash
# Check MongoDB status
sudo systemctl status mongod

# Start MongoDB
sudo systemctl start mongod

# Verify connection
mongosh --eval "db.adminCommand('ping')"
```

#### 3. No Matches Found for Date

**Cause**: Wrong date format or no matches scheduled

**Solution**:
```bash
# Use correct format YYYYMMDD
python daily_pipeline.py -d 20241215  # ✅ Correct
python daily_pipeline.py -d 2024-12-15  # ❌ Wrong

# Check status first
python daily_pipeline.py -d 20241215 --status
```

#### 4. Pipeline Interrupted

**Cause**: Network issue or manual stop

**Solution**:
```bash
# Historical pipeline: Just run again - it will resume automatically
python pipeline.py

# Check progress
python pipeline.py --status

# Daily pipeline: Re-run for the same date
python daily_pipeline.py -d 20241215
```

#### 5. Safe Update Blocked

**Cause**: Daily pipeline detected existing data, won't overwrite

**Solution**:
```bash
# Use --force to override (use with caution)
python daily_pipeline.py -d 20241215 --force
```

#### 6. ID Type Mismatch

**Cause**: Querying with string IDs instead of integers

**Solution**:
```python
# Wrong
queries.get_league_by_id("47")

# Correct
queries.get_league_by_id(47)
```

### Logs

Check logs for detailed error information:

```bash
# Historical pipeline logs
tail -100 logs/pipeline_log.txt
grep -i error logs/pipeline_log.txt

# Daily pipeline logs
tail -100 logs/daily_pipeline.log
grep -i error logs/daily_pipeline.log

# Search for specific match
grep "4813566" logs/daily_pipeline.log
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Run linting
flake8 .
black . --check
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- [FotMob](https://www.fotmob.com/) for the comprehensive football data API
- [MongoDB](https://www.mongodb.com/) for the flexible document database
- [Pydantic](https://pydantic.dev/) for data validation

---

## 📧 Contact

For questions or support, please open an issue on GitHub.

---

**Happy Coding!** ⚽🚀
