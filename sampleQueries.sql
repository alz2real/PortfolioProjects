-- **************************************************************************************************** --
-- ************************************ANALYSIS******************************************************** --
-- **************************************************************************************************** --

/*
Query team games data for all available teams:
*/
COPY
(
	SELECT
		DISTINCT ti.team_id AS team_id,
		g.game_id,
		g.venue AS venue,
		CONCAT(ti.shortname,' ', ti.teamname) AS team,
		g.match_date AS match_date,
		gts.hoa AS hoa,
		gts.won AS won,
		gts.shots AS shots,
		gts.power_play_opportunities AS power_play_opportunities,
		gts.power_play_goals AS power_play_goals,
		gts.face_off_win_percentage AS face_off_win_percentage,
		gts.giveaways AS giveaways,
		gts.takeaways AS takeaways,
		gts.blocked AS blocked
	FROM game_teams_stats gts
	JOIN team_info ti
	ON ti.team_id = gts.team_id
	JOIN game g
	ON gts.game_id = g.game_id
	WHERE ti.team_id IN 
	(
		SELECT team_id 
		FROM team_info
	)
	AND g.match_date BETWEEN '2011-01-01' AND '2020-12-01'
	ORDER BY game_id, g.match_date DESC
)
TO 'D:/E-Resource/Data_and_AI/NHL_Game_Data/analysis/all_teams_summary.csv' WITH CSV HEADER;
-- 24212 Copied

/*
Query team games from 2010 to 2020 for the following Canadian province teams: Winnipeg, Vancouver, 
Ottawa, Montreal, Toronto, Calgary, Edmonton. Save results to a system folder for visualization
*/
COPY
(
	SELECT
		DISTINCT ti.team_id AS team_id,
		g.game_id,
		g.venue AS venue,
		CONCAT(ti.shortname,' ', ti.teamname) AS team,
		g.match_date AS match_date,
		gts.hoa AS hoa,
		gts.won AS won,
		gts.shots AS shots,
		gts.power_play_opportunities AS power_play_opportunities,
		gts.power_play_goals AS power_play_goals,
		gts.face_off_win_percentage AS face_off_win_percentage,
		gts.giveaways AS giveaways,
		gts.takeaways AS takeaways,
		gts.blocked AS blocked
	FROM game_teams_stats gts
	JOIN team_info ti
	ON ti.team_id = gts.team_id
	JOIN game g
	ON gts.game_id = g.game_id
	WHERE ti.team_id IN 
	(
		SELECT team_id 
		FROM team_info
		WHERE shortname IN ('Winnipeg', 'Vancouver', 'Ottawa', 'Montreal', 'Toronto', 'Calgary', 'Edmonton')
	)
	AND g.match_date BETWEEN '2011-01-01' AND '2020-12-01'
	ORDER BY game_id, g.match_date DESC
)
TO 'D:/E-Resource/Data_and_AI/NHL_Game_Data/analysis/canadian_teams_summary.csv' WITH CSV HEADER;
-- 5389 Copied

-- Query below pulls out goalie stats for Canadian teams from 2011 to 2020
COPY	
(
	SELECT 
		ggs.player_id AS goalie_id,
		CONCAT(pi.firstname, ' ', pi.lastname) AS goalie_name,
		CONCAT(ti.shortname, ' ', ti.teamname) AS team,
		g.match_date,
		ggs.timeonice,
		ggs.saves,
		ggs.powerplaysaves
	FROM game_goalie_stats ggs
	JOIN team_info ti
	ON ggs.team_id = ti.team_id
	JOIN player_info pi
	ON ggs.player_id = pi.player_id
	JOIN game g
	ON ggs.game_id = g.game_id
	WHERE ti.shortname IN ('Winnipeg', 'Vancouver', 'Ottawa', 'Montreal', 'Toronto', 'Calgary', 'Edmonton')
	AND g.match_date BETWEEN '2011-01-01' AND '2020-12-01'
	ORDER BY saves DESC
)
TO 'D:/E-Resource/Data_and_AI/NHL_Game_Data/analysis/goalie_performance.csv' WITH CSV HEADER;	
-- 6977 Copied

-- Query below pulls out skater stats for Canadian teams from 2011 to 2020
COPY	
(
	SELECT 
		gss.player_id AS skater_id,
		CONCAT(pi.firstname, ' ', pi.lastname) AS skater_name,
		CONCAT(ti.shortname, ' ', ti.teamname) AS team,
		g.match_date,
		gss.assists,
		gss.goals,
		gss.shots
	FROM game_skater_stats gss
	JOIN team_info ti
	ON gss.team_id = ti.team_id
	JOIN player_info pi
	ON gss.player_id = pi.player_id
	JOIN game g
	ON gss.game_id = g.game_id
	WHERE ti.shortname IN ('Winnipeg', 'Vancouver', 'Ottawa', 'Montreal', 'Toronto', 'Calgary', 'Edmonton')
	AND g.match_date BETWEEN '2011-01-01' AND '2020-12-01'
	ORDER BY goals DESC
)
TO 'D:/E-Resource/Data_and_AI/NHL_Game_Data/analysis/skater_performance.csv' WITH CSV HEADER;	
-- 117260 Copied

