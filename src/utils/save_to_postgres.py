import pandas as pd
from sqlalchemy import create_engine
from typing import Dict

def save_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    config: Dict[str, str],
    if_exists: str = 'append',
    index: bool = False
) -> None:
    """
    Save DataFrame to PostgreSQL database
    
    Args:
        df: DataFrame to save
        table_name: Target table name
        config: Dictionary with database configuration
        if_exists: What to do if table exists ('fail', 'replace', 'append')
        index: Whether to write DataFrame index as a column
    """
    try:
        engine = create_engine(
            f"postgresql://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )
        
        with engine.connect() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists=if_exists,
                index=index,
                method='multi'
            )
        
        print(f"Successfully saved {len(df)} records to {table_name}")
    except Exception as e:
        print(f"Error saving to PostgreSQL: {str(e)}")
        raise