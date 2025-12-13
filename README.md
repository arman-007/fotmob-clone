# ⚽ Football Stats Pipeline

A robust, production-ready data pipeline for fetching and storing comprehensive football (soccer) statistics from FotMob API. Designed for Fantasy Premier League-style applications with MongoDB integration, checkpoint/resume functionality, and flexible query helpers.

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
- [Database Schema](#database-schema)
- [Query Helpers API](#query-helpers-api)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

The Football Stats Pipeline is a comprehensive data ingestion system that:

- **Fetches** league, season, match, and player statistics from FotMob
- **Stores** data in MongoDB with optimized schema for Fantasy PL-style queries
- **Supports** checkpoint/resume for handling interruptions gracefully
- **Provides** ready-to-use query helpers for building web applications

### Use Cases

- Building Fantasy Football applications
- Sports analytics dashboards
- Player performance tracking systems
- Historical football data analysis
- Machine learning datasets for sports predictions

---

## ✨ Features

### Core Features

| Feature | Description |
|---------|-------------|
| **Multi-League Support** | Fetches data from 500+ leagues worldwide |
| **Historical Data** | Retrieves up to 10 seasons per league |
| **Player Statistics** | Comprehensive stats including goals, assists, ratings, cards |
| **Match Details** | Full match information with player-level granularity |
| **Team Data** | Team profiles with aggregated statistics |

### Pipeline Features

| Feature | Description |
|---------|-------------|
| **Checkpoint/Resume** | Automatically resumes from where it stopped |
| **Failed Match Retry** | Tracks and retries failed matches |
| **Progress Tracking** | Real-time progress monitoring via `--status` |
| **Dual Storage** | JSON files (debugging) + MongoDB (production) |
| **Flexible Execution** | Multiple CLI flags for customization |

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
│                        PIPELINE FLOW                            │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   FotMob     │───▶│   Pipeline   │───▶│   MongoDB    │
│     API      │    │  Orchestrator│    │   Database   │
└──────────────┘    └──────────────┘    └──────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  JSON Files  │
                    │  (Optional)  │
                    └──────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      DATA FLOW                                  │
└─────────────────────────────────────────────────────────────────┘

1. Capture X-MAS Token ──▶ Authentication for API requests
           │
           ▼
2. Fetch All Leagues ────▶ 537 leagues from 200+ countries
           │
           ▼
3. For Each League:
   ├── Fetch Season Data ──▶ Last 10 seasons
   │          │
   │          ▼
   └── For Each Season:
       ├── Get Match IDs ──▶ 380+ matches per season
       │          │
       │          ▼
       └── For Each Match:
           └── Fetch Player Stats ──▶ 22+ players per match
                      │
                      ▼
              ┌───────┴───────┐
              ▼               ▼
         MongoDB          JSON Files
```

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

### Basic Commands

```bash
# Run pipeline (default: save to both JSON and MongoDB)
python pipeline.py

# Check pipeline progress
python pipeline.py --status

# Skip JSON files (faster, production mode)
python pipeline.py --no-json

# Process limited leagues (for testing)
python pipeline.py --league-limit 2

# Force re-process all data
python pipeline.py --force

# Retry only failed matches
python pipeline.py --retry-failed

# Build player profiles after ingestion
python pipeline.py --build-players
```

### CLI Options Reference

| Flag | Description | Example |
|------|-------------|---------|
| `--no-json` | Skip saving JSON files | `python pipeline.py --no-json` |
| `--no-mongodb` | Skip MongoDB (JSON only) | `python pipeline.py --no-mongodb` |
| `--league-limit N` | Process only N leagues | `python pipeline.py --league-limit 5` |
| `--force` | Ignore checkpoints, re-process all | `python pipeline.py --force` |
| `--retry-failed` | Only retry failed matches | `python pipeline.py --retry-failed` |
| `--status` | Show progress and exit | `python pipeline.py --status` |
| `--build-players` | Build player profiles | `python pipeline.py --build-players` |
| `-d, --date` | Date parameter | `python pipeline.py -d 20241213` |

### Combined Examples

```bash
# Production run: no JSON, all leagues
python pipeline.py --no-json

# Development: 2 leagues, with JSON for debugging
python pipeline.py --league-limit 2

# Resume after interruption (automatic)
python pipeline.py

# Full re-run with player profiles
python pipeline.py --force --build-players

# Check status and database stats
python pipeline.py --status
```

### Status Output Example

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

❌ Failed Matches (first 10):
   - League 47, Season 2024-2025, Match 4521342
     Error: 403 Client Error: Forbidden...

============================================================

==================================================
DATABASE STATISTICS
==================================================
  leagues: 537 documents
  seasons: 450 documents
  matches: 12,450 documents
  player_stats: 498,000 documents
  players: 25,000 documents
  teams: 1,200 documents
==================================================
```

---

## 🗄️ Database Schema

### Collections Overview

| Collection | Purpose | Key Fields |
|------------|---------|------------|
| `leagues` | League metadata | `league_id`, `name`, `country_code` |
| `seasons` | Season information | `league_season_key`, `league_id`, `season_id` |
| `matches` | Match details | `match_id`, `home_team`, `away_team`, `player_stats` |
| `player_stats` | Per-match player stats | `player_match_key`, `player_id`, `match_id` |
| `players` | Aggregated player profiles | `player_id`, `total_goals`, `total_assists` |
| `teams` | Team information | `team_id`, `name` |
| `pipeline_state` | Checkpoint tracking | `league_id`, `season_id`, `status` |

### Sample Documents

#### leagues
```javascript
{
  "league_id": "47",
  "name": "Premier League",
  "localized_name": "Premier League",
  "country_code": "ENG",
  "page_url": "https://www.fotmob.com/leagues/47/overview/premier-league",
  "category": "popular",
  "created_at": ISODate("2024-12-13T10:00:00Z"),
  "updated_at": ISODate("2024-12-13T10:00:00Z")
}
```

#### player_stats
```javascript
{
  "player_match_key": "961995_4521342",
  "player_id": "961995",
  "name": "Mohamed Salah",
  "team_id": "8650",
  "team_name": "Liverpool",
  "match_id": "4521342",
  "match_datetime_utc": ISODate("2024-12-07T15:00:00Z"),
  "league_id": "47",
  "season_id": "2024-2025",
  "league_season_key": "47_2024-2025",
  "opponent_team_id": "8455",
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

#### pipeline_state
```javascript
{
  "_id": "47_2024-2025",
  "league_id": "47",
  "season_id": "2024-2025",
  "status": "completed",
  "total_matches": 380,
  "processed_matches": ["4521342", "4521343", ...],
  "failed_matches": [],
  "started_at": ISODate("2024-12-13T10:00:00Z"),
  "completed_at": ISODate("2024-12-13T12:30:00Z"),
  "last_updated": ISODate("2024-12-13T12:30:00Z")
}
```

### Indexes

Optimized indexes are automatically created for:

- Primary key lookups (`league_id`, `match_id`, `player_id`)
- Composite queries (`league_season_key` + `match_datetime_utc`)
- Aggregation pipelines (goals, assists, rating)
- Text search (player names, team names)

---

## 🔍 Query Helpers API

The `QueryHelpers` class provides ready-to-use methods for your application:

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

# Get single league
league = queries.get_league_by_id("47")

# Get seasons for a league
seasons = queries.get_league_seasons("47")
```

#### Match Queries

```python
# Get matches for league/season
matches = queries.get_matches_for_league_season("47", "2024-2025", finished_only=True)

# Get single match with player stats
match = queries.get_match_by_id("4521342", include_player_stats=True)

# Get recent matches
recent = queries.get_recent_matches(league_id="47", days=7, limit=20)

# Get team matches
team_matches = queries.get_team_matches("8650", league_season_key="47_2024-2025")
```

#### Player Stats Queries

```python
# Get player stats
stats = queries.get_player_stats("961995", league_season_key="47_2024-2025")

# Get player form (last N matches)
form = queries.get_player_form("961995", matches=5)

# Get top scorers
top_scorers = queries.get_top_scorers("47", "2024-2025", limit=20)

# Get top assists
top_assists = queries.get_top_assists("47", "2024-2025", limit=20)

# Get top rated players
top_rated = queries.get_top_rated_players("47", "2024-2025", min_matches=5, limit=20)

# Compare players
comparison = queries.compare_players(
    player_ids=["961995", "237079"],
    league_season_key="47_2024-2025"
)

# Get player season summary
summary = queries.get_player_season_summary("961995", "47_2024-2025")
```

#### Team Queries

```python
# Get team by ID
team = queries.get_team_by_id("8650")

# Get team players with stats
players = queries.get_team_players("8650", "47_2024-2025")

# Get team season stats
team_stats = queries.get_team_season_stats("8650", "47_2024-2025")
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
├── pipeline.py              # Main orchestration script
├── get_additional_stats.py  # Additional stats processor
├── requirements.txt         # Python dependencies
├── .env                     # Environment configuration
├── .env.example             # Example environment file
├── README.md                # This file
│
├── service/                 # API interaction modules
│   ├── __init__.py
│   ├── get_auth_headers.py  # X-MAS token capture
│   ├── get_leagues.py       # League fetching
│   ├── get_specific_league.py  # Season data fetching
│   └── get_player_stats.py  # Player stats fetching
│
├── db/                      # Database modules
│   ├── __init__.py
│   ├── mongodb_service.py   # MongoDB operations
│   ├── validators.py        # Pydantic validators
│   ├── query_helpers.py     # Query helper functions
│   └── pipeline_state.py    # Checkpoint/resume system
│
├── utils/                   # Utility modules
│   ├── __init__.py
│   ├── get_all_season_match_ids.py  # Match ID extraction
│   └── get_timezone.py      # Timezone utilities
│
├── logs/                    # Log files
│   └── pipeline_log.txt
│
└── output/                  # JSON output (optional)
    └── leagues/
        └── {league_id}/
            └── {season}/
                ├── league_info_*.json
                └── player_stats/
                    └── player_stats_matchID_*.json
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

#### 3. Season Validation Errors

**Cause**: Season format mismatch

**Solution**: The pipeline now supports all formats:
- `2024-2025` (full year range)
- `2024-25` (short year range)
- `2024` (single year - World Cup, etc.)

#### 4. Pipeline Interrupted

**Cause**: Network issue or manual stop

**Solution**:
```bash
# Just run again - it will resume automatically
python pipeline.py

# Check progress
python pipeline.py --status
```

#### 5. Out of Memory

**Cause**: Processing too many leagues at once

**Solution**:
```bash
# Process in batches
python pipeline.py --league-limit 10
# Run again for next batch (automatic resume)
python pipeline.py --league-limit 20
```

### Logs

Check logs for detailed error information:

```bash
# View recent logs
tail -100 logs/pipeline_log.txt

# Search for errors
grep -i error logs/pipeline_log.txt

# Search for specific league
grep "league 47" logs/pipeline_log.txt
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
