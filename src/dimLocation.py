from typing import Dict
import pandas as pd
from utils.read_csv import read_csv_file
from utils.save_to_postgres import save_to_postgres
import logging

def dimensionLocationETL(config: Dict[str, str]) -> Dict[str, int]:
    """
    ETL process for location dimension
    
    Args:
        config: Configuration dictionary containing database connection info
        
    Returns:
        Dictionary mapping location key (city + arena) to surrogate key (id)
    """
    try:
        # Read team data from CSV to extract location information
        df = read_csv_file('./data/teams.csv', {
            'TEAM_ID': int,
            'CITY': str,
            'ARENA': str,
            'ARENACAPACITY': float
        })

        # Handle missing arena capacity values
        df["ARENACAPACITY"] = df["ARENACAPACITY"].fillna(-1).astype(int)

        # Create unique locations by grouping by city and arena
        # Some cities might have multiple arenas, so we keep them separate
        location_df = df[['CITY', 'ARENA', 'ARENACAPACITY']].drop_duplicates()
        
        # Create DataFrame for location dimension
        df_save = pd.DataFrame({
            "city": location_df["CITY"],
            "arena": location_df["ARENA"],
            "arena_capacity": location_df["ARENACAPACITY"]
        })

        # Add surrogate key (id)
        df_save['id'] = df_save.reset_index().index + 1
        df_save.reset_index(drop=True, inplace=True)

        # Save to PostgreSQL
        save_to_postgres(
            df=df_save,
            table_name='dim_location',
            config=config,
        )

        logging.info(f"Successfully processed {len(df_save)} locations")

        # Return mapping {city + arena -> id}
        # We'll use city + arena as the key since some cities have multiple arenas
        location_mapping = {}
        for _, row in df_save.iterrows():
            key = f"{row['city']}|{row['arena']}"
            location_mapping[key] = row['id']
        
        return location_mapping

    except Exception as e:
        logging.error(f"Error in location dimension ETL: {str(e)}")
        raise
