import pandas as pd
import logging
from typing import Dict, Union

def read_csv_file(file_path: str, dtype: Dict[str, Union[str, int, float]] = None) -> pd.DataFrame:
    """
    Read CSV file into a pandas DataFrame with error handling
    
    Args:
        file_path: Path to the CSV file
        dtype: Dictionary specifying column data types
        
    Returns:
        pandas DataFrame with the loaded data
    """
    try:
        df = pd.read_csv(
            file_path,
            dtype=dtype,
            parse_dates=True,
            low_memory=False
        )
        logging.info(f"Successfully loaded {len(df)} records from {file_path}")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {str(e)}")
        raise