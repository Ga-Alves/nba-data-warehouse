from typing import Dict
import pandas as pd
from utils.read_csv import read_csv_file
from utils.save_to_postgres import save_to_postgres
import logging

def dimensionGameETL(config: Dict[str, str], team_mapping: Dict[int, int]) -> Dict[int, int]:
    """
    ETL process for game dimension

    Args:
        config: Database config
        team_mapping: Dict with {original_team_id: surrogate_id} from dim_team

    Returns:
        Dict with {original_game_id: surrogate_key}
    """
    try:
        # Lê o arquivo games.csv
        df = read_csv_file('./data/games.csv', {
            'GAME_ID': int,
            'SEASON': int,
            'HOME_TEAM_ID': int,
            'VISITOR_TEAM_ID': int,
            'PTS_home': float,
            'FG_PCT_home': float,
            'FT_PCT_home': float,
            'FG3_PCT_home': float,
            'AST_home': float,
            'REB_home': float,
            'PTS_away': float,
            'FG_PCT_away': float,
            'FT_PCT_away': float,
            'FG3_PCT_away': float,
            'AST_away': float,
            'REB_away': float,
            'HOME_TEAM_WINS': bool
        })

        # Mapeia os IDs dos times
        df["home_team_id"] = df["HOME_TEAM_ID"].map(team_mapping)
        df["visitor_team_id"] = df["VISITOR_TEAM_ID"].map(team_mapping)

        # Verifica se houve falhas no mapeamento
        missing_home = df[df['home_team_id'].isnull()]['HOME_TEAM_ID'].unique()
        missing_visitor = df[df['visitor_team_id'].isnull()]['VISITOR_TEAM_ID'].unique()
        if len(missing_home) > 0 or len(missing_visitor) > 0:
            logging.warning(f"Times não encontrados na dimensão Team: {set(missing_home) | set(missing_visitor)}")

        # Cria DataFrame final
        df_save = pd.DataFrame({
            "game_id": df["GAME_ID"],
            "season": df["SEASON"],
            "home_team_id": df["home_team_id"],
            "visitor_team_id": df["visitor_team_id"],
            "points_home": df["PTS_home"].fillna(0).astype(int),
            "field_goal_percentage_home": df["FG_PCT_home"].fillna(0.0),
            "free_throw_percentage_home": df["FT_PCT_home"].fillna(0.0),
            "three_point_percentage_home": df["FG3_PCT_home"].fillna(0.0),
            "assists_home": df["AST_home"].fillna(0).astype(int),
            "rebounds_home": df["REB_home"].fillna(0).astype(int),
            "points_visitor": df["PTS_away"].fillna(0).astype(int),
            "field_goal_percentage_visitor": df["FG_PCT_away"].fillna(0.0),
            "free_throw_percentage_visitor": df["FT_PCT_away"].fillna(0.0),
            "three_point_percentage_visitor": df["FG3_PCT_away"].fillna(0.0),
            "assists_visitor": df["AST_away"].fillna(0).astype(int),
            "rebounds_visitor": df["REB_away"].fillna(0).astype(int),
            "does_home_team_wins": df["HOME_TEAM_WINS"].fillna(False).astype(bool),
        })

        # Cria surrogate key
        df_save["id"] = df_save.reset_index().index + 1

        # Salva no banco
        save_to_postgres(
            df=df_save,
            table_name='dim_game',
            config=config,
        )

        logging.info(f"Successfully processed {len(df_save)} games")

        # Retorna mapeamento GAME_ID → surrogate id
        return dict(zip(df_save["game_id"], df_save["id"]))

    except Exception as e:
        logging.error(f"Error in game dimension ETL: {str(e)}")
        raise
