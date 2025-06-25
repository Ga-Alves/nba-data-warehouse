from datetime import date
from typing import Dict

def factETL(config: Dict[str, Dict[str, str]], date_mapping:Dict[date, int], player_mapping:Dict[int, int], team_mapping:Dict[int, int], game_mapping:Dict[int, int]):
    print("fact ETL")
