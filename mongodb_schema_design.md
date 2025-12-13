# MongoDB Schema Design for Football Stats Platform

## Overview

This design uses a **Hybrid approach** optimized for:
- Fantasy Premier League-style website
- Read-heavy analytics workload
- Both match-centric and player-centric queries

---

## Collections

### 1. `leagues`

Stores league metadata. Relatively static data.

```javascript
{
  _id: ObjectId,
  league_id: "47",                    // FotMob league ID (indexed, unique)
  name: "Premier League",
  localized_name: "Premier League",
  country_code: "ENG",
  page_url: "/leagues/47/overview/premier-league",
  
  // Denormalized season summary for quick access
  seasons: [
    {
      season_id: "2024-25",
      is_current: true
    },
    {
      season_id: "2023-24",
      is_current: false
    }
    // ... up to 10 seasons
  ],
  
  created_at: ISODate,
  updated_at: ISODate
}
```

**Indexes:**
```javascript
db.leagues.createIndex({ "league_id": 1 }, { unique: true })
db.leagues.createIndex({ "country_code": 1 })
db.leagues.createIndex({ "name": "text" })  // Text search
```

---

### 2. `seasons`

Stores season-specific data per league.

```javascript
{
  _id: ObjectId,
  league_id: "47",
  season_id: "2024-25",              // Normalized format (YYYY-YY)
  
  // Combined unique identifier
  league_season_key: "47_2024-25",   // For quick lookups (indexed, unique)
  
  // Season metadata from league_info JSON
  league_name: "Premier League",
  country_code: "ENG",
  
  // Stats summary (can be computed/updated periodically)
  stats_summary: {
    total_matches: 380,
    completed_matches: 250,
    total_goals: 620,
    top_scorer: {
      player_id: "961995",
      name: "Erling Haaland",
      goals: 20
    }
  },
  
  created_at: ISODate,
  updated_at: ISODate
}
```

**Indexes:**
```javascript
db.seasons.createIndex({ "league_season_key": 1 }, { unique: true })
db.seasons.createIndex({ "league_id": 1, "season_id": -1 })
db.seasons.createIndex({ "season_id": 1 })  // Query all leagues for a season
```

---

### 3. `matches`

Stores match information with embedded player stats array.

```javascript
{
  _id: ObjectId,
  match_id: "4193490",               // FotMob match ID (indexed, unique)
  
  // Foreign keys
  league_id: "47",
  season_id: "2024-25",
  league_season_key: "47_2024-25",   // Denormalized for efficient queries
  
  // Match metadata
  match_name: "Arsenal vs Chelsea",
  match_datetime_utc: ISODate("2024-12-15T15:00:00Z"),
  
  // Status
  started: true,
  finished: true,
  
  // Teams
  home_team: {
    team_id: "9825",
    name: "Arsenal",
    score: 2
  },
  away_team: {
    team_id: "8455",
    name: "Chelsea", 
    score: 1
  },
  
  // Embedded player stats (for match-centric queries)
  // This allows fetching all match data in a single query
  player_stats: [
    {
      player_id: "961995",
      name: "Bukayo Saka",
      team_id: "9825",
      team_name: "Arsenal",
      is_goalkeeper: false,
      
      // Performance stats
      minutes_played: 90,
      goals: 1,
      assists: 1,
      expected_goals: 0.85,
      expected_assists: 0.72,
      shots: 4,
      shots_on_target: 2,
      
      // Passing
      accurate_passes: "45/52 (86.5%)",
      key_passes: 3,
      
      // Defensive
      tackles_won: 2,
      interceptions: 1,
      clearances: 0,
      
      // Discipline
      yellow_card: false,
      red_card: false,
      
      // Rating
      rating: 8.2
      
      // ... other stats from your _further_process_player_stats
    }
    // ... ~40 players per match
  ],
  
  // Match-level aggregates (for quick queries)
  stats_summary: {
    total_goals: 3,
    total_yellow_cards: 4,
    total_red_cards: 0
  },
  
  created_at: ISODate,
  updated_at: ISODate
}
```

**Indexes:**
```javascript
// Primary lookups
db.matches.createIndex({ "match_id": 1 }, { unique: true })

// League/Season queries (most common)
db.matches.createIndex({ "league_id": 1, "season_id": -1, "match_datetime_utc": -1 })
db.matches.createIndex({ "league_season_key": 1, "match_datetime_utc": -1 })

// Team queries
db.matches.createIndex({ "home_team.team_id": 1, "match_datetime_utc": -1 })
db.matches.createIndex({ "away_team.team_id": 1, "match_datetime_utc": -1 })

// Date range queries
db.matches.createIndex({ "match_datetime_utc": -1 })

// Player lookup within matches (for aggregation pipelines)
db.matches.createIndex({ "player_stats.player_id": 1 })

// Status filters
db.matches.createIndex({ "finished": 1, "league_season_key": 1 })
```

---

### 4. `player_stats`

**Flattened player-match stats** for efficient player-centric queries.
This is the key to supporting Fantasy PL-style analytics.

```javascript
{
  _id: ObjectId,
  
  // Composite key for deduplication
  player_match_key: "961995_4193490",  // player_id_match_id (indexed, unique)
  
  // Player info
  player_id: "961995",
  name: "Bukayo Saka",
  team_id: "9825",
  team_name: "Arsenal",
  is_goalkeeper: false,
  
  // Match context (denormalized for efficient queries)
  match_id: "4193490",
  match_datetime_utc: ISODate("2024-12-15T15:00:00Z"),
  league_id: "47",
  season_id: "2024-25",
  league_season_key: "47_2024-25",
  
  // Opponent info (useful for fantasy analysis)
  opponent_team_id: "8455",
  opponent_team_name: "Chelsea",
  is_home: true,
  
  // All stats (same structure as in matches.player_stats)
  minutes_played: 90,
  goals: 1,
  assists: 1,
  expected_goals: 0.85,
  expected_assists: 0.72,
  shots: 4,
  shots_on_target: 2,
  accurate_passes: "45/52 (86.5%)",
  key_passes: 3,
  tackles_won: 2,
  interceptions: 1,
  clearances: 0,
  yellow_card: false,
  red_card: false,
  rating: 8.2,
  
  // ... all other stats
  
  created_at: ISODate
}
```

**Indexes:**
```javascript
// Unique constraint
db.player_stats.createIndex({ "player_match_key": 1 }, { unique: true })

// Player queries (most important for Fantasy PL)
db.player_stats.createIndex({ "player_id": 1, "match_datetime_utc": -1 })
db.player_stats.createIndex({ "player_id": 1, "league_season_key": 1, "match_datetime_utc": -1 })

// Team queries
db.player_stats.createIndex({ "team_id": 1, "match_datetime_utc": -1 })

// League/Season aggregations (top scorers, etc.)
db.player_stats.createIndex({ "league_season_key": 1, "goals": -1 })
db.player_stats.createIndex({ "league_season_key": 1, "assists": -1 })
db.player_stats.createIndex({ "league_season_key": 1, "rating": -1 })

// Match lookup
db.player_stats.createIndex({ "match_id": 1 })

// Player name search
db.player_stats.createIndex({ "name": "text" })
```

---

### 5. `players` (Aggregated Player Profiles)

Denormalized player profiles for quick profile page loads.
Can be computed/updated after data ingestion.

```javascript
{
  _id: ObjectId,
  player_id: "961995",               // FotMob player ID (indexed, unique)
  name: "Bukayo Saka",
  
  // Current team (latest known)
  current_team_id: "9825",
  current_team_name: "Arsenal",
  is_goalkeeper: false,
  
  // Career summary
  total_matches: 245,
  total_goals: 52,
  total_assists: 48,
  
  // Per-season breakdown
  seasons: [
    {
      league_id: "47",
      league_name: "Premier League",
      season_id: "2024-25",
      team_id: "9825",
      team_name: "Arsenal",
      matches: 18,
      goals: 8,
      assists: 10,
      avg_rating: 7.45,
      minutes_played: 1520
    },
    {
      league_id: "47",
      league_name: "Premier League",
      season_id: "2023-24",
      team_id: "9825",
      team_name: "Arsenal",
      matches: 35,
      goals: 16,
      assists: 12,
      avg_rating: 7.62,
      minutes_played: 3010
    }
    // ... historical seasons
  ],
  
  // Recent form (last 5 matches) - useful for Fantasy PL
  recent_form: [
    { match_id: "4193490", goals: 1, assists: 1, rating: 8.2 },
    { match_id: "4193385", goals: 0, assists: 2, rating: 7.8 },
    // ... last 5
  ],
  
  created_at: ISODate,
  updated_at: ISODate
}
```

**Indexes:**
```javascript
db.players.createIndex({ "player_id": 1 }, { unique: true })
db.players.createIndex({ "current_team_id": 1 })
db.players.createIndex({ "name": "text" })
db.players.createIndex({ "total_goals": -1 })  // Career leaders
db.players.createIndex({ "seasons.league_season_key": 1 })
```

---

### 6. `teams` (Optional but Recommended)

```javascript
{
  _id: ObjectId,
  team_id: "9825",
  name: "Arsenal",
  
  // Leagues this team participates in
  leagues: [
    { league_id: "47", league_name: "Premier League" },
    { league_id: "73", league_name: "FA Cup" }
  ],
  
  created_at: ISODate,
  updated_at: ISODate
}
```

**Indexes:**
```javascript
db.teams.createIndex({ "team_id": 1 }, { unique: true })
db.teams.createIndex({ "name": "text" })
```

---

## Common Query Examples

### 1. Get all matches for a specific league/season

```javascript
// Fast: Uses league_season_key index
db.matches.find({
  league_season_key: "47_2024-25"
})
.sort({ match_datetime_utc: -1 })
.limit(20)
```

### 2. Get all stats for a specific player across matches/seasons

```javascript
// Using player_stats collection (optimized for this)
db.player_stats.find({
  player_id: "961995"
})
.sort({ match_datetime_utc: -1 })

// With season filter
db.player_stats.find({
  player_id: "961995",
  league_season_key: "47_2024-25"
})
.sort({ match_datetime_utc: -1 })
```

### 3. Get all players' stats for a specific match

```javascript
// Option A: From matches collection (single document read)
db.matches.findOne(
  { match_id: "4193490" },
  { player_stats: 1, home_team: 1, away_team: 1 }
)

// Option B: From player_stats collection
db.player_stats.find({ match_id: "4193490" })
```

### 4. Top scorers in a league/season

```javascript
db.player_stats.aggregate([
  { $match: { league_season_key: "47_2024-25" } },
  { 
    $group: {
      _id: "$player_id",
      name: { $first: "$name" },
      team_name: { $last: "$team_name" },
      total_goals: { $sum: "$goals" },
      matches_played: { $sum: 1 },
      total_assists: { $sum: "$assists" }
    }
  },
  { $sort: { total_goals: -1 } },
  { $limit: 20 }
])
```

### 5. Player comparison (Fantasy PL use case)

```javascript
db.player_stats.aggregate([
  { 
    $match: { 
      player_id: { $in: ["961995", "232348"] },
      league_season_key: "47_2024-25"
    }
  },
  {
    $group: {
      _id: "$player_id",
      name: { $first: "$name" },
      matches: { $sum: 1 },
      goals: { $sum: "$goals" },
      assists: { $sum: "$assists" },
      avg_rating: { $avg: "$rating" },
      total_minutes: { $sum: "$minutes_played" }
    }
  }
])
```

### 6. Player form (last 5 matches)

```javascript
db.player_stats.find({
  player_id: "961995"
})
.sort({ match_datetime_utc: -1 })
.limit(5)
.project({
  match_id: 1,
  match_datetime_utc: 1,
  opponent_team_name: 1,
  goals: 1,
  assists: 1,
  rating: 1
})
```

### 7. Team's recent results

```javascript
db.matches.find({
  $or: [
    { "home_team.team_id": "9825" },
    { "away_team.team_id": "9825" }
  ],
  finished: true
})
.sort({ match_datetime_utc: -1 })
.limit(10)
```

---

## Data Ingestion Strategy

### Order of Operations

1. **Insert Leagues** (from `leagues_data.json`)
2. **Insert Seasons** (from `league_info_*.json` files)
3. **Insert Matches + Player Stats** (from `player_stats_*.json` files)
   - Insert into `matches` collection (with embedded player_stats)
   - Insert into `player_stats` collection (flattened records)
4. **Build Players Collection** (aggregation from player_stats)

### Why Dual Storage (matches + player_stats)?

| Use Case | Best Collection |
|----------|-----------------|
| Display single match | `matches` (1 read) |
| Player profile page | `player_stats` (indexed by player_id) |
| Top scorers leaderboard | `player_stats` (pre-indexed aggregation) |
| Match list for league | `matches` (indexed by league_season_key) |

The ~4MB overhead of duplicated data is worth the query performance gain.

---

## Storage Estimates

| Collection | Documents | Avg Doc Size | Total Size |
|------------|-----------|--------------|------------|
| leagues | 100 | 1 KB | ~100 KB |
| seasons | 1,000 | 2 KB | ~2 MB |
| matches | 100,000 | 40 KB | ~4 GB |
| player_stats | 4,000,000 | 1 KB | ~4 GB |
| players | ~50,000 | 5 KB | ~250 MB |
| **Total** | | | **~8-10 GB** |

This is very manageable for local MongoDB.

---

## Index Memory Requirements

Estimated index sizes:
- leagues: < 1 MB
- seasons: < 5 MB
- matches: ~200 MB
- player_stats: ~800 MB
- players: ~50 MB

**Total index memory: ~1 GB**

Ensure your MongoDB instance has sufficient RAM (4GB+ recommended).
