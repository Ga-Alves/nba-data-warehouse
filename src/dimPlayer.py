from typing import Dict
import pandas as pd
from utils.read_csv import read_csv_file
from utils.save_to_postgres import save_to_postgres
import logging

def dimensionPlayerETL(config:  Dict[str, str]) -> Dict[int, int]:
    """
    ETL process for player dimension
    
    Args:
        config: Configuration dictionary containing database connection info
        
    Returns:
        Dictionary mapping original player_id to surrogate key (id)
    """
    try:
        # Read player data from CSV
        df = read_csv_file('../data/players.csv', {
            'PLAYER_NAME': str,
            'TEAM_ID': int,
            'PLAYER_ID': int,
            'SEASON': int,
        })

        # Create DataFrame for saving with required columns
        df_save = pd.DataFrame({
            "player_id": df['PLAYER_ID'],
            "name": df['PLAYER_NAME']
        })

        # Add surrogate key
        df_save['id'] = df_save.reset_index().index + 1

        # Save to database
        save_to_postgres(
            df=df_save,
            table_name='dim_player',
            config=config,
        )

        logging.info(f"Successfully processed {len(df_save)} players")
        
        # Create and return mapping dictionary {original_id: surrogate_key}
        player_mapping = dict(zip(df_save['player_id'], df_save['id']))
        return player_mapping

    except Exception as e:
        logging.error(f"Error in player dimension ETL: {str(e)}")
        raise