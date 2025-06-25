import configparser
from typing import Dict
from dimDate import dimensionDateCreation
from dimGame import dimensionGameETL
from dimPlayer import dimensionPlayerETL
from dimTeam import dimensionTeamETL
from factPlayerGameStatistics import factETL


def load_config() -> Dict[str, Dict[str, str]]:
    """Load configuration from config.ini"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    return {
        'database': {
            'host': config['postgres']['host'],
            'user': config['postgres']['user'],
            'password': config['postgres']['password'],
            'database': config['postgres']['databaseName'],
            'port': config['postgres'].get('port', '5432')  # Default PostgreSQL port
        },
    }

def main():
    try:
        # Load configuration
        config = load_config()
        
        print("Starting ETL process")
        
        # TODO: parallelize this
        # Parallel ETL processes
        date_mapping = dimensionDateCreation(config['database'])
        player_mapping = dimensionPlayerETL(config['database'])
        
        # Sequential steps with dependencies
        team_mapping = dimensionTeamETL(config['database'])
        game_mapping = dimensionGameETL(config['database'], team_mapping)
        
        # Fact table with all mappings
        factETL(
            config['database'],
            date_mapping={},
            player_mapping=player_mapping,
            team_mapping=team_mapping,
            game_mapping=game_mapping
        )
        
        print("ETL process completed successfully")
        
    except Exception as e:
        print(f"ETL process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()