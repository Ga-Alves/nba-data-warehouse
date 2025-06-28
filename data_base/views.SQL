/*
VISÕES PARA ANÁLISE DE ESTATÍSTICAS DE JOGADORES DE BASQUETE

1. vw_player_stats_summary:
   - Estatísticas consolidadas por jogador
   - Mostra totais e médias de pontos, rebotes, assistências e outros indicadores
   - Inclui porcentagens de arremesso (campo, 3 pontos e lances livres)

2. vw_player_performance_by_location:
   - Desempenho dos jogadores por local (cidade/arena)
   - Compara médias de pontos, rebotes e assistências em diferentes locais
   - Útil para identificar onde o jogador tem melhor desempenho

3. vw_team_season_stats:
   - Estatísticas de times por temporada
   - Mostra vitórias, médias de pontos e porcentagens de acerto
   - Compara desempenho como mandante e visitante

4. vw_player_performance_by_weekday:
   - Desempenho dos jogadores por dia da semana
   - Revela padrões de performance em diferentes dias
   - Ordenado por jogador e dia da semana (1-7)

5. vw_top_players:
   - Ranking dos melhores jogadores
   - Filtra apenas jogadores com 5+ jogos
   - Mostras médias em todas as estatísticas principais
   - Inclui porcentagens de arremesso formatadas (0-100%)

6. vw_player_efficiency:
   - Métricas avançadas de eficiência
   - Calcula Game Efficiency Rating e True Shooting Percentage
   - Mostra eficiência global do jogador

7. vw_home_away_performance:
   - Comparação de desempenho em casa vs fora
   - Divide estatísticas por tipo de jogo (Home/Away)
   - Revela como o jogador se adapta a diferentes ambientes

8. vw_player_monthly_progression:
   - Progressão mensal do desempenho
   - Mostra evolução das estatísticas ao longo do tempo
   - Agrupado por jogador, ano e mês para análise temporal
*/

CREATE OR REPLACE VIEW vw_player_stats_summary AS
SELECT 
    p.name AS player_name,
    t.nickname AS team_name,
    COUNT(DISTINCT f.game_id) AS games_played,
    (AVG(f.minutes_played))::numeric(10,2) AS avg_minutes_played,
    SUM(f.points_scored) AS total_points,
    (AVG(f.points_scored))::numeric(10,2) AS avg_points,
    SUM(f.field_goals_made) AS total_field_goals,
    SUM(f.field_goals_attempt) AS total_field_goal_attempts,
    (AVG(f.field_goals_average)*100)::numeric(10,2) AS avg_field_goal_percentage,
    SUM(f.three_points_made) AS total_three_points,
    SUM(f.three_goals_attempt) AS total_three_point_attempts,
    (AVG(f.three_goals_average)*100)::numeric(10,2) AS avg_three_point_percentage,
    SUM(f.rebounds) AS total_rebounds,
    SUM(f.assists) AS total_assists,
    SUM(f.steals) AS total_steals,
    SUM(f.blocked_shots) AS total_blocks,
    SUM(f.turn_over) AS total_turnovers,
    SUM(f.personal_foul) AS total_fouls
FROM 
    fact_player_game_statistics f
JOIN 
    dim_player p ON f.player_id = p.id
JOIN 
    dim_team t ON f.team_id = t.id
GROUP BY 
    p.name, t.nickname;

CREATE OR REPLACE VIEW vw_player_performance_by_location AS
SELECT 
    p.name AS player_name,
    l.city,
    l.arena,
    COUNT(*) AS games_played,
    (AVG(f.points_scored))::numeric(10,2) AS avg_points,
    (AVG(f.rebounds))::numeric(10,2) AS avg_rebounds,
    (AVG(f.assists))::numeric(10,2) AS avg_assists,
    (AVG(f.field_goals_average)*100)::numeric(10,2) AS avg_fg_percentage
FROM 
    fact_player_game_statistics f
JOIN 
    dim_player p ON f.player_id = p.id
JOIN 
    dim_location l ON f.location_id = l.id
GROUP BY 
    p.name, l.city, l.arena;

CREATE OR REPLACE VIEW vw_team_season_stats AS
SELECT 
    t.nickname AS team_name,
    g.season,
    COUNT(DISTINCT g.id) AS games_played,
    SUM(CASE WHEN g.does_home_team_wins AND g.home_team_id = t.id THEN 1
              WHEN NOT g.does_home_team_wins AND g.visitor_team_id = t.id THEN 1
              ELSE 0 END) AS wins,
    (AVG(CASE WHEN g.home_team_id = t.id THEN g.points_home ELSE g.points_visitor END))::numeric(10,2) AS avg_points,
    (AVG(CASE WHEN g.home_team_id = t.id THEN g.field_goal_percentage_home ELSE g.field_goal_percentage_visitor END)*100)::numeric(10,2) AS avg_fg_percentage,
    (AVG(CASE WHEN g.home_team_id = t.id THEN g.three_point_percentage_home ELSE g.three_point_percentage_visitor END)*100)::numeric(10,2) AS avg_3pt_percentage,
    (AVG(CASE WHEN g.home_team_id = t.id THEN g.assists_home ELSE g.assists_visitor END))::numeric(10,2) AS avg_assists,
    (AVG(CASE WHEN g.home_team_id = t.id THEN g.rebounds_home ELSE g.rebounds_visitor END))::numeric(10,2) AS avg_rebounds
FROM 
    dim_game g
JOIN 
    dim_team t ON g.home_team_id = t.id OR g.visitor_team_id = t.id
GROUP BY 
    t.nickname, g.season;

CREATE OR REPLACE VIEW vw_player_performance_by_weekday AS
SELECT 
    p.name AS player_name,
    d.day_name AS weekday,
    d.day_of_week,
    COUNT(*) AS games_played,
    (AVG(f.points_scored))::numeric(10,2) AS avg_points,
    (AVG(f.rebounds))::numeric(10,2) AS avg_rebounds,
    (AVG(f.assists))::numeric(10,2) AS avg_assists,
    (AVG(f.minutes_played))::numeric(10,2) AS avg_minutes
FROM 
    fact_player_game_statistics f
JOIN 
    dim_player p ON f.player_id = p.id
JOIN 
    dim_date d ON f.date_id = d.id
GROUP BY 
    p.name, d.day_name, d.day_of_week
ORDER BY 
    p.name, d.day_of_week;

CREATE OR REPLACE VIEW vw_top_players AS
SELECT 
    p.name AS player_name,
    t.nickname AS team_name,
    (AVG(f.points_scored))::numeric(10,2) AS avg_points,
    (AVG(f.rebounds))::numeric(10,2) AS avg_rebounds,
    (AVG(f.assists))::numeric(10,2) AS avg_assists,
    (AVG(f.steals))::numeric(10,2) AS avg_steals,
    (AVG(f.blocked_shots))::numeric(10,2) AS avg_blocks,
    (AVG(f.field_goals_average)*100)::numeric(10,2) AS avg_fg_percentage,
    (AVG(f.three_goals_average)*100)::numeric(10,2) AS avg_3pt_percentage
FROM 
    fact_player_game_statistics f
JOIN 
    dim_player p ON f.player_id = p.id
JOIN 
    dim_team t ON f.team_id = t.id
GROUP BY 
    p.name, t.nickname
HAVING 
    COUNT(DISTINCT f.game_id) >= 5;


CREATE OR REPLACE VIEW vw_player_efficiency AS
SELECT 
    p.name AS player_name,
    t.nickname AS team_name,
    COUNT(DISTINCT f.game_id) AS games_played,
    ROUND(SUM(f.points_scored + f.rebounds + f.assists + f.steals + f.blocked_shots - (f.field_goals_attempt - f.field_goals_made) - (f.free_throws_attempt - f.free_throws_made) - f.turn_over) / COUNT(DISTINCT f.game_id), 2) AS game_efficiency_rating,
    ROUND(SUM(f.points_scored) / NULLIF(SUM(f.field_goals_attempt + 0.44 * f.free_throws_attempt + f.turn_over), 0), 2) AS true_shooting_percentage
FROM 
    fact_player_game_statistics f
JOIN 
    dim_player p ON f.player_id = p.id
JOIN 
    dim_team t ON f.team_id = t.id
GROUP BY 
    p.name, t.nickname;

CREATE OR REPLACE VIEW vw_home_away_performance AS
SELECT 
    p.name AS player_name,
    CASE WHEN t.id = g.home_team_id THEN 'Home' ELSE 'Away' END AS game_type,
    COUNT(*) AS games_played,
    (AVG(f.points_scored))::numeric(10,2) AS avg_points,
    (AVG(f.rebounds))::numeric(10,2) AS avg_rebounds,
    (AVG(f.assists))::numeric(10,2) AS avg_assists,
    (AVG(f.field_goals_average)*100)::numeric(10,2) AS avg_fg_percentage
FROM 
    fact_player_game_statistics f
JOIN 
    dim_player p ON f.player_id = p.id
JOIN 
    dim_team t ON f.team_id = t.id
JOIN 
    dim_game g ON f.game_id = g.id
GROUP BY 
    p.name, game_type;


CREATE OR REPLACE VIEW vw_player_monthly_progression AS
SELECT 
    p.name AS player_name,
    d.year,
    d.month,
    COUNT(*) AS games_played,
    (AVG(f.points_scored))::numeric(10,2) AS avg_points,
    (AVG(f.rebounds))::numeric(10,2) AS avg_rebounds,
    (AVG(f.assists))::numeric(10,2) AS avg_assists,
    (AVG(f.field_goals_average)*100)::numeric(10,2) AS avg_fg_percentage
FROM 
    fact_player_game_statistics f
JOIN 
    dim_player p ON f.player_id = p.id
JOIN 
    dim_date d ON f.date_id = d.id
GROUP BY 
    p.name, d.year, d.month
ORDER BY 
    p.name, d.year, d.month;



