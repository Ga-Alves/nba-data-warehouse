CREATE TABLE fact_player_game_statistics (
  id UInt32,
  game_id UInt32,
  player_id UInt32,
  date_id UInt32,
  team_id UInt32,
  location_id UInt32,
  start_position String,
  minutes_played Float32,
  field_goals_made UInt16,
  field_goals_attempt UInt16,
  field_goals_average Float32,
  three_points_made UInt16,
  three_goals_attempt UInt16,
  three_goals_average Float32,
  free_throws_made UInt16,
  free_throws_attempt UInt16,
  free_throws_average Float32,
  rebounds UInt16,
  defensive_rebounds UInt16,
  assists UInt16,
  steals UInt16,
  blocked_shots UInt16,
  turn_over UInt16,
  personal_foul UInt16,
  points_scored UInt16,
  plus_minus Int16
) ENGINE = MergeTree()
ORDER BY (date_id, game_id, player_id);

CREATE TABLE dim_game (
  id UInt32,
  home_team_id UInt32,
  visitor_team_id UInt32,
  game_id UInt32,
  season UInt16,
  points_home UInt16,
  field_goal_percentage_home Float32,
  free_throw_percentage_home Float32,
  three_point_percentage_home Float32,
  assists_home UInt16,
  rebounds_home UInt16,
  points_visitor UInt16,
  field_goal_percentage_visitor Float32,
  free_throw_percentage_visitor Float32,
  three_point_percentage_visitor Float32,
  assists_visitor UInt16,
  rebounds_visitor UInt16,
  does_home_team_wins Bool
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE dim_player (
  id UInt32,
  player_id UInt32,
  name String
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE dim_team (
  id UInt32,
  team_id UInt32,
  min_nba_year UInt16,
  max_nba_year UInt16,
  abbreviation String,
  nickname String,
  founded_year UInt16,
  location_id UInt32,
  last_owner String,
  general_manager String,
  head_coach String,
  league_affiliation String
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE dim_date (
  id UInt32,
  date Date,
  year UInt16,
  month UInt8,
  day UInt8,
  day_of_week UInt8,
  day_name String,
  week_of_year UInt8,
  day_of_year UInt16,
  is_holiday Bool,
  holiday_name String
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE dim_location (
  id UInt32,
  city String,
  arena String,
  arena_capacity Int32
) ENGINE = MergeTree()
ORDER BY id;

-- ClickHouse doesn't support traditional foreign key constraints
-- Referential integrity must be maintained at the application level
-- 
-- Foreign key relationships (for documentation purposes):
-- fact_player_game_statistics.game_id -> dim_game.id
-- fact_player_game_statistics.player_id -> dim_player.id
-- fact_player_game_statistics.date_id -> dim_date.id
-- fact_player_game_statistics.team_id -> dim_team.id
-- fact_player_game_statistics.location_id -> dim_location.id
-- dim_game.home_team_id -> dim_team.id
-- dim_game.visitor_team_id -> dim_team.id
-- dim_team.location_id -> dim_location.id
