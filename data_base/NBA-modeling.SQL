CREATE TABLE "fact_player_game_statistics" (
  "id" int PRIMARY KEY,
  "game_id" int,
  "player_id" int,
  "date_id" int,
  "team_id" int,
  "location_id" int,
  "start_position" varchar,
  "minutes_played" float,
  "field_goals_made" int,
  "field_goals_attempt" int,
  "field_goals_average" float,
  "three_points_made" int,
  "three_goals_attempt" int,
  "three_goals_average" float,
  "free_throws_made" int,
  "free_throws_attempt" int,
  "free_throws_average" float,
  "rebounds" int,
  "defensive_rebounds" int,
  "assists" int,
  "steals" int,
  "blocked_shots" int,
  "turn_over" int,
  "personal_foul" int,
  "points_scored" int,
  "plus_minus" int
);

CREATE TABLE "dim_game" (
  "id" int PRIMARY KEY,
  "home_team_id" int,
  "visitor_team_id" int,
  "game_id" int,
  "season" int,
  "points_home" int,
  "field_goal_percentage_home" float,
  "free_throw_percentage_home" float,
  "three_point_percentage_home" float,
  "assists_home" int,
  "rebounds_home" int,
  "points_visitor" int,
  "field_goal_percentage_visitor" float,
  "free_throw_percentage_visitor" float,
  "three_point_percentage_visitor" float,
  "assists_visitor" int,
  "rebounds_visitor" int,
  "does_home_team_wins" bool
);

CREATE TABLE "dim_player" (
  "id" int PRIMARY KEY,
  "player_id" int,
  "name" varchar
);

CREATE TABLE "dim_team" (
  "id" int PRIMARY KEY,
  "team_id" int,
  "min_nba_year" int,
  "max_nba_year" int,
  "abbreviation" varchar,
  "nickname" varchar,
  "founded_year" int,
  "location_id" int,
  "last_owner" varchar,
  "general_manager" varchar,
  "head_coach" varchar,
  "league_affiliation" varchar
);

CREATE TABLE "dim_date" (
  "id" int PRIMARY KEY,
  "date" date,
  "year" int,
  "month" int,
  "day" int,
  "day_of_week" int,
  "day_name" varchar,
  "week_of_year" int,
  "day_of_year" int,
  "is_holiday" bool,
  "holiday_name" varchar
);

CREATE TABLE "dim_location" (
  "id" int PRIMARY KEY,
  "city" varchar,
  "arena" varchar,
  "arena_capacity" int
);

ALTER TABLE "fact_player_game_statistics" ADD FOREIGN KEY ("game_id") REFERENCES "dim_game" ("id");

ALTER TABLE "dim_game" ADD FOREIGN KEY ("home_team_id") REFERENCES "dim_team" ("id");

ALTER TABLE "dim_game" ADD FOREIGN KEY ("visitor_team_id") REFERENCES "dim_team" ("id");

ALTER TABLE "fact_player_game_statistics" ADD FOREIGN KEY ("date_id") REFERENCES "dim_date" ("id");

ALTER TABLE "fact_player_game_statistics" ADD FOREIGN KEY ("player_id") REFERENCES "dim_player" ("id");

ALTER TABLE "fact_player_game_statistics" ADD FOREIGN KEY ("team_id") REFERENCES "dim_team" ("id");

ALTER TABLE "fact_player_game_statistics" ADD FOREIGN KEY ("location_id") REFERENCES "dim_location" ("id");

ALTER TABLE "dim_team" ADD FOREIGN KEY ("location_id") REFERENCES "dim_location" ("id");
