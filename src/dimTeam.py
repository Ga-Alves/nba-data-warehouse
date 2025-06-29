from typing import Dict
import pandas as pd
from utils.read_csv import read_csv_file
from utils.save_to_postgres import save_to_postgres
import logging

def dimensionTeamETL(config: Dict[str, str], location_mapping: Dict[str, int]) -> Dict[int, int]:
    """
    ETL process for team dimension
    
    Args:
        config: Configuration dictionary containing database connection info
        location_mapping: Dictionary mapping location key to location dimension surrogate key
        
    Returns:
        Dictionary mapping original team_id to surrogate key (id)
    """
    try:
        # Read team data from CSV
        df = read_csv_file('../data/teams.csv', {
            'TEAM_ID': int,
            'MIN_YEAR': int,
            'MAX_YEAR': int,
            'ABBREVIATION': str,
            'NICKNAME': str,
            'YEARFOUNDED': int,
            'CITY': str,
            'ARENA': str,
            'ARENACAPACITY': float,
            'OWNER': str,
            'GENERALMANAGER': str,
            'HEADCOACH': str,
            'DLEAGUEAFFILIATION': str
        })

        df["ARENACAPACITY"] = df["ARENACAPACITY"].fillna(-1).astype(int)

        # Map location to location dimension surrogate key
        df['location_key'] = df['CITY'] + '|' + df['ARENA']
        df['location_id'] = df['location_key'].map(location_mapping)

        # Create DataFrame with desired columns
        df_save = pd.DataFrame({
            "team_id": df["TEAM_ID"],
            "min_nba_year": df["MIN_YEAR"],
            "max_nba_year": df["MAX_YEAR"],
            "abbreviation": df["ABBREVIATION"],
            "nickname": df["NICKNAME"],
            "founded_year": df["YEARFOUNDED"],
            "location_id": df["location_id"],
            "last_owner": df["OWNER"],
            "general_manager": df["GENERALMANAGER"],
            "head_coach": df["HEADCOACH"],
            "league_affiliation": df["DLEAGUEAFFILIATION"]
        })

        # Add surrogate key (id)
        df_save['id'] = df_save.reset_index().index + 1

        # Save to PostgreSQL
        # save_to_postgres(
        #     df=df_save,
        #     table_name='dim_team',
        #     config=config,
        # )
        df_save.to_csv('../data/derived/dim_team.csv', index=False)
        logging.info(f"Successfully processed {len(df_save)} teams")

        # Return mapping {team_id original -> id substituta}
        team_mapping = dict(zip(df_save['team_id'], df_save['id']))
        return team_mapping

    except Exception as e:
        logging.error(f"Error in team dimension ETL: {str(e)}")
        raise