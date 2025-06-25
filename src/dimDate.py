# dimDate.py
import pandas as pd
import holidays
from datetime import date
from typing import Dict
from utils.save_to_postgres import save_to_postgres
import logging

def generate_date_dimension(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Generate date dimension table with US holiday information
    
    Args:
        start_date: Start date of the dimension
        end_date: End date of the dimension
        
    Returns:
        DataFrame with date dimension data
    """
    try:
        # Create date range
        dates = pd.date_range(start_date, end_date)
        
        # Get US holidays
        us_holidays = holidays.US(years=range(start_date.year, end_date.year + 1))
        holiday_dates = pd.to_datetime(list(us_holidays.keys()))
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': dates,
            'year': dates.year,
            'month': dates.month,
            'day': dates.day,
            'day_of_week': dates.dayofweek,  # 0=Monday, 6=Sunday
            'day_name': dates.day_name(),
            'week_of_year': dates.isocalendar().week,
            'day_of_year': dates.dayofyear,
            'is_holiday': dates.isin(holiday_dates),
            'holiday_name': dates.map(lambda d: us_holidays.get(d, ''))
        })
        
        # Add surrogate key
        df['id'] = df.reset_index().index + 1
        
        return df
    
    except Exception as e:
        logging.error(f"Error generating date dimension: {str(e)}")
        raise

def dimensionDateETL(config: Dict[str, str]) -> Dict[date, int]:
    """
    ETL process for date dimension
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Dictionary mapping dates to surrogate keys
    """
    try:
        # Define date range (adjust as needed)
        start_date = date(2003, 1, 1)  # NBA data typically goes back to 1940s
        end_date = date(2022, 12, 31)  # Adjust based on your needs
        
        # Generate dimension
        date_df = generate_date_dimension(start_date, end_date)
        
        # Save to database
        save_to_postgres(
            df=date_df,
            table_name='dim_date',
            config=config,
            if_exists='replace'
        )
        
        # Create mapping dictionary
        date_mapping = dict(zip(date_df['date'], date_df['id']))
        
        return date_mapping
    
    except Exception as e:
        logging.error(f"Error in date dimension ETL: {str(e)}")
        raise