from datetime import date
from typing import Dict
import pandas as pd
from utils.read_csv import read_csv_file
from utils.save_to_postgres import save_to_postgres
import logging


def factETL(config: Dict[str, str], date_mapping: Dict[date, int], player_mapping: Dict[int, int], team_mapping: Dict[int, int], game_mapping: Dict[int, int], location_mapping: Dict[str, int]):
    """
    ETL process for player game statistics fact table
    
    Args:
        config: Database configuration
        date_mapping: Dict mapping dates to date dimension surrogate keys
        player_mapping: Dict mapping player IDs to player dimension surrogate keys
        team_mapping: Dict mapping team IDs to team dimension surrogate keys
        game_mapping: Dict mapping game IDs to game dimension surrogate keys
        location_mapping: Dict mapping location keys to location dimension surrogate keys
    """
    try:
        logging.info("Starting fact table ETL process...")
        
        # Read games_details.csv file
        df = read_csv_file('./data/games_details.csv', {
            'GAME_ID': int,
            'TEAM_ID': int,
            'TEAM_ABBREVIATION': str,
            'TEAM_CITY': str,
            'PLAYER_ID': int,
            'PLAYER_NAME': str,
            'NICKNAME': str,
            'START_POSITION': str,
            'COMMENT': str,
            'MIN': str,  # Minutes played as string (e.g., "18:06")
            'FGM': float,  # Field goals made
            'FGA': float,  # Field goals attempted
            'FG_PCT': float,  # Field goal percentage
            'FG3M': float,  # 3-point field goals made
            'FG3A': float,  # 3-point field goals attempted
            'FG3_PCT': float,  # 3-point percentage
            'FTM': float,  # Free throws made
            'FTA': float,  # Free throws attempted
            'FT_PCT': float,  # Free throw percentage
            'OREB': float,  # Offensive rebounds
            'DREB': float,  # Defensive rebounds
            'REB': float,  # Total rebounds
            'AST': float,  # Assists
            'STL': float,  # Steals
            'BLK': float,  # Blocks
            'TO': float,  # Turnovers
            'PF': float,  # Personal fouls
            'PTS': float,  # Points
            'PLUS_MINUS': float  # Plus/minus
        })
        
        logging.info(f"Loaded {len(df)} player game statistics records")
        
        # Convert minutes from "MM:SS" format to integer minutes
        def convert_minutes(min_str):
            if pd.isna(min_str) or min_str == '':
                return 0
            try:
                minutes, _ = map(int, min_str.split(':'))
                return minutes
            except ValueError:
                return 0
            
        
        df['minutes_decimal'] = df['MIN'].apply(convert_minutes)
        # # Map foreign keys to surrogate keys
        df['game_surrogate_id'] = df['GAME_ID'].map(game_mapping)
        df['player_surrogate_id'] = df['PLAYER_ID'].map(player_mapping)
        df['team_surrogate_id'] = df['TEAM_ID'].map(team_mapping)
        
        # For date mapping, we need to get the game date from the games.csv
        # First, let's read the games.csv to get the mapping of GAME_ID to GAME_DATE_EST
        games_df = read_csv_file('./data/games.csv', {
            'GAME_ID': int,
            'GAME_DATE_EST': str
        })
        
        # Convert game dates to date objects
        games_df['game_date'] = pd.to_datetime(games_df['GAME_DATE_EST']).dt.date
        game_date_mapping = dict(zip(games_df['GAME_ID'], games_df['game_date']))
        
        # Map game dates and then to date surrogate keys
        df['game_date'] = df['GAME_ID'].map(game_date_mapping)
        df['date_surrogate_id'] = df['game_date'].map(date_mapping)
        
        # Get location information from teams.csv to map location_id
        teams_df = read_csv_file('./data/teams.csv', {
            'TEAM_ID': int,
            'CITY': str,
            'ARENA': str
        })
        
        # Create team to location mapping
        teams_df['location_key'] = teams_df['CITY'] + '|' + teams_df['ARENA']
        team_location_mapping = dict(zip(teams_df['TEAM_ID'], teams_df['location_key']))
        
        # Map team to location key and then to location surrogate key
        df['location_key'] = df['TEAM_ID'].map(team_location_mapping)
        df['location_surrogate_id'] = df['location_key'].map(location_mapping)
        
        # Check for missing mappings
        missing_games = df[df['game_surrogate_id'].isnull()]['GAME_ID'].nunique()
        missing_players = df[df['player_surrogate_id'].isnull()]['PLAYER_ID'].nunique()
        missing_teams = df[df['team_surrogate_id'].isnull()]['TEAM_ID'].nunique()
        missing_dates = df[df['date_surrogate_id'].isnull()]['GAME_ID'].nunique()
        missing_locations = df[df['location_surrogate_id'].isnull()]['TEAM_ID'].nunique()
        
        if missing_games > 0:
            logging.warning(f"Missing game mappings for {missing_games} unique games")
        if missing_players > 0:
            logging.warning(f"Missing player mappings for {missing_players} unique players")
        if missing_teams > 0:
            logging.warning(f"Missing team mappings for {missing_teams} unique teams")
        if missing_dates > 0:
            logging.warning(f"Missing date mappings for {missing_dates} unique games")
        if missing_locations > 0:
            logging.warning(f"Missing location mappings for {missing_locations} unique teams")
        
        # Drop rows with missing critical mappings
        initial_count = len(df)
        df = df.dropna(subset=['game_surrogate_id', 'player_surrogate_id', 'team_surrogate_id', 'date_surrogate_id', 'location_surrogate_id'])
        final_count = len(df)
        
        if initial_count != final_count:
            logging.warning(f"Dropped {initial_count - final_count} rows due to missing mappings")
        
        # Create the fact table DataFrame
        fact_df = pd.DataFrame({
            'game_id': df['game_surrogate_id'].astype(int),
            'player_id': df['player_surrogate_id'].astype(int),
            'date_id': df['date_surrogate_id'].astype(int),
            'team_id': df['team_surrogate_id'].astype(int),
            'location_id': df['location_surrogate_id'].astype(int),
            'start_position': df['START_POSITION'].fillna(''),
            'minutes_played': df['minutes_decimal'],
            
            # Field goals
            'field_goals_made': df['FGM'].fillna(0).astype(int),
            'field_goals_attempt': df['FGA'].fillna(0).astype(int),
            'field_goals_average': df['FG_PCT'].fillna(0.0),
            
            # Three-point shots
            'three_points_made': df['FG3M'].fillna(0).astype(int),
            'three_goals_attempt': df['FG3A'].fillna(0).astype(int),
            'three_goals_average': df['FG3_PCT'].fillna(0.0),
            
            # Free throws
            'free_throwss_made': df['FTM'].fillna(0).astype(int),
            'free_throws_attempt': df['FTA'].fillna(0).astype(int),
            'free_throws_average': df['FT_PCT'].fillna(0.0),
            
            # Rebounds
            'rebounds': df['REB'].fillna(0).astype(int),
            'defensive_rebounds': df['DREB'].fillna(0).astype(int),
            
            # Other statistics
            'assists': df['AST'].fillna(0).astype(int),
            'steals': df['STL'].fillna(0).astype(int),
            'blocked_shots': df['BLK'].fillna(0).astype(int),
            'turn_over': df['TO'].fillna(0).astype(int),
            'personal_foul': df['PF'].fillna(0).astype(int),
            'points_scored': df['PTS'].fillna(0).astype(int),
            'plus_minus': df['PLUS_MINUS'].fillna(0).astype(int)
        })
        # Add surrogate key
        fact_df['id'] = fact_df.reset_index().index + 1
        fact_df.reset_index(drop=True, inplace=True)
        
        # Process in batches to avoid memory issues
        batch_size = 10000  # Process 10K records at a time
        total_records = len(fact_df)
        
        logging.info(f"Processing {total_records} records in batches of {batch_size}")
        
        # Save to database in batches
        for i in range(0, total_records, batch_size):
            batch_end = min(i + batch_size, total_records)
            batch_df = fact_df.iloc[i:batch_end].copy()
            
            # Update surrogate keys for this batch to ensure uniqueness
            batch_df['id'] = range(i + 1, batch_end + 1)
            
            logging.info(f"Saving batch {i//batch_size + 1}: records {i+1} to {batch_end}")
            
            # For the first batch, create/replace the table. For subsequent batches, append
            if i == 0:
                save_to_postgres(
                    df=batch_df,
                    table_name='fact_player_game_statistics',
                    config=config,
                    if_exists='replace'
                )
            else:
                # For subsequent batches, append to the existing table
                save_to_postgres(
                    df=batch_df,
                    table_name='fact_player_game_statistics',
                    config=config,
                    if_exists='append'
                )
            
            # Clear memory
            del batch_df
            
            # Log progress
            progress = (batch_end / total_records) * 100
            logging.info(f"Progress: {progress:.1f}% ({batch_end}/{total_records} records)")
        
        logging.info(f"Successfully processed all {total_records} player game statistics records")
        
    except Exception as e:
        logging.error(f"Error in fact table ETL: {str(e)}")
        raise
