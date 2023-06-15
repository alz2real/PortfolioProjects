/*
The data used for this project was sourced from https://www.kaggle.com/datasets/martinellis/nhl-game-data
It consists of 7 datasets with official metrics measured for each game from year 2000 to 2020.
I started by viewing each of the downloaded csv file in a text editor. This allows me to quicky apply 
filters and get broad overview of the nature of each dataset, including the existence of nulls/blanks.
*/
-- After observation of the dataset in spreasheet, tables are created in postgresql to house the data

-- **************************************************************************************************** --

-- Create the game table:
CREATE TABLE public.game
(
    game_id integer,
    season integer,
    type character(1),
    date_time character varying, -- this is imported as a character type for ease of import. To be converted afterwards.
    away_team_id smallint,
    home_team_id smallint,
    away_goals smallint,
    home_goals smallint,
    outcome character varying,
    home_rink_side_start character varying,
    venue character varying,
    venue_link character varying,
    venue_time_zone_id character varying,
    venue_time_zone_offset smallint,
    venue_time_zone_tz character(3)
)
TABLESPACE pg_default;

ALTER TABLE public.game
OWNER to postgres;

-- Import game data from game.csv into the newly created game table:
COPY game FROM 'D:/E-Resource/Data_and_AI/NHL_Game_Data/game.csv' WITH CSV HEADER DELIMITER ',';
--26305 Records Copied

-- During observation in spreadsheet, it was noticed that game_id has a number of duplicates
-- Identify records with duplicate game_id:
SELECT game_id, COUNT(game_id) AS num_of_dup
FROM game
GROUP BY game_id
HAVING COUNT(game_id) > 1;
-- 2570 records .
-- The ideal number of unique records should thus be 26305 - 2570 = 23735 records

-- Create a new table with no duplicates:
CREATE TABLE game_distinct
AS
SELECT DISTINCT *
FROM game;

-- Confirm there are only 23735 records in the newly create table:
SELECT COUNT(*)
FROM game_distinct;
-- 23735

-- Drop table with duplicates:
DROP TABLE game;

-- Rename table game_distinct to game:
ALTER TABLE game_distinct
RENAME TO game;

COMMIT; -- Save Changes

-- Observe the first 10 rows of the table:
SELECT * 
FROM game
LIMIT 10;

/*
The date_time column is currently imported as a character type. The following steps are
taken to convert this column to separate date and time columns:
1 - Extract date into a separate column called match_date, and time into a column called match_time
Example: 2016-10-19T01:30:00Z is separated into 2016-10-19 and 01:30
*/
-- Create a temporary table with the new columns:
CREATE TABLE game_temp AS
SELECT *,
SUBSTRING(date_time, 1, 10) AS match_date,
SUBSTRING(date_time, 12, 5) AS match_time
FROM game;

-- Observe the first 10 rows of the temporary table:
SELECT * 
FROM game_temp
LIMIT 10;

-- Drop the game table:
DROP TABLE game;

/*
2 - Change the datatype of the match_date column from character to date:
*/
ALTER TABLE game_temp
ALTER COLUMN match_date TYPE DATE USING TO_DATE(match_date, 'YYYY-MM-DD');

/*
3 - Change the datatype of the match_time column from character to time:
*/

ALTER TABLE game_temp
ALTER COLUMN match_time TYPE TIME USING match_time::TIME;

/*
4 - Drop the date_time column:
*/
ALTER TABLE game_temp
DROP COLUMN date_time;

-- Query the default position of columns in the game_temp table:
SELECT attnum, attname
FROM pg_attribute
WHERE attrelid = 'game_temp'::regclass
  AND attnum > 0
  AND NOT attisdropped
ORDER BY attnum;
-- match_date and match_time are currently the last two columns - positions 16 and 17

-- Create a new table game with columns in the desired order:
CREATE TABLE game AS
SELECT
game_id,
season,
type,
match_date,
match_time,
away_team_id,
home_team_id,
away_goals,
home_goals,
outcome,
home_rink_side_start,
venue,
venue_link,
venue_time_zone_id,
venue_time_zone_offset,
venue_time_zone_tz
FROM game_temp;

-- Observe the first 10 rows, column positions and datatypes of the new table:
SELECT * 
FROM game
LIMIT 10;

/*
Apply a primary key constraint to the game_id column to ensure unique values and 
to enforce referential integrity during imports of other tables
*/
ALTER TABLE game
ADD CONSTRAINT game_id_pkey PRIMARY KEY (game_id);

-- Drop the temporary table:
DROP TABLE game_temp;

COMMIT; -- Save changes

-- Export the cleaned game table to a system folder:
COPY 
(
	SELECT * 
	FROM game
	ORDER BY match_date
) 
TO 'D:/E-Resource/Data_and_AI/NHL_Game_Data/clean_data/game_cleaned.csv' WITH CSV HEADER;
-- 23735 Copied

-- **************************************************************************************************** --

-- Create the game_plays table:
CREATE TABLE public.game_plays
(
    play_id character varying,
    game_id integer,
    team_id_for character varying,
    team_id_against character varying,
    event character varying,
    secondaryType character varying,
    x character varying,
    y character varying,
    period smallint,
    periodType character varying,
    periodTime smallint,
    periodTimeRemaining character varying,  -- this is imported as a character type due to the presence of NA. To be converted afterwards.
    dateTime character varying,  -- this is imported as a character type for ease of import. To be converted afterwards.
    goals_away smallint,
    goals_home smallint,
    description character varying,
    st_x character varying,
    st_y character varying,
    CONSTRAINT "game_id_Fkey" FOREIGN KEY (game_id)
        REFERENCES public.game (game_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
)
TABLESPACE pg_default;

ALTER TABLE public.game_plays
    OWNER to postgres;
	

-- Import game_plays data from game_plays.csv into the newly created game_plays table:
COPY game_plays FROM 'D:/E-Resource/Data_and_AI/NHL_Game_Data/game_plays.csv' WITH CSV HEADER DELIMITER ',';
-- 5050529 Records Copied

-- Add a primary key constraint to the play_id column:
ALTER TABLE game_plays
ADD CONSTRAINT game_play_Pkey PRIMARY KEY (play_id);

-- Observe the first 10 rows of the table:
SELECT * 
FROM game_plays
LIMIT 10;

-- Check for existence of duplicates in play_id column:
SELECT play_id, COUNT(play_id) AS num_of_dup
FROM game_plays
GROUP BY play_id
HAVING COUNT(play_id) > 1;
-- 833466 Records
-- The ideal number of unique records should thus be 5050529 - 833466 = 4217063 records

-- Export a copy of the duplicate records to a local system folder:
COPY
(
	WITH dup_records AS
		(
			SELECT play_id, COUNT(play_id) AS num_of_dup
			FROM game_plays
			GROUP BY play_id
			HAVING COUNT(play_id) > 1
		)
	SELECT *
	FROM game_plays
	WHERE play_id IN(SELECT play_id FROM dup_records)
	ORDER BY play_id
)
TO 'D:/E-Resource/Data_and_AI/NHL_Game_Data/dup_records/game_plays_dups.csv' WITH CSV HEADER;
-- 1666932 Records Copied. This is exactly 2X 833466

-- Create a new table with no duplicates:
CREATE TABLE game_plays_distinct
AS
SELECT DISTINCT *
FROM game_plays;

-- Confirm there are only 4217063 records in the newly create table:
SELECT COUNT(play_id)
FROM game_plays_distinct;
-- 4217063

-- Drop game_plays table:
DROP table game_plays;

COMMIT; -- Save changes

-- Observe the first 10 rows of the new table:
SELECT * 
FROM game_plays_distinct
LIMIT 10;

-- Apply a primary key constraint on play_id column to improve data retrieval performmance:
ALTER TABLE game_plays_distinct
ADD CONSTRAINT play_id_pkey PRIMARY KEY (play_id);

-- Rename the table back to game_plays
ALTER TABLE game_plays_distinct
RENAME TO game_plays;

-- Count the number of occurences of 'NA' in team_id_for column:
SELECT COUNT(*) 
FROM game_plays
WHERE team_id_for = 'NA';
-- 775247 

-- Set all occurences of 'NA' in team_id_for to null
UPDATE game_plays
SET team_id_for = NULL
WHERE team_id_for = 'NA';

-- Change the datatype of team_id_for from character to smallint
ALTER TABLE game_plays
ALTER COLUMN team_id_for TYPE smallint
USING team_id_for::smallint;

/*
All occurence of 'NA' in the following columns are set to nulls: team_id_against, secondaryType
x, y, periodTimeRemaining, st_x, and st_y
*/
UPDATE game_plays
SET 
	team_id_against = NULL,
	secondaryType = NULL,
	periodTimeRemaining = NULL,
	x = NULL,
	y = NULL,
	st_x = NULL,
	st_y = NULL
WHERE
	team_id_against = 'NA' OR
	secondaryType = 'NA' OR
	periodTimeRemaining = 'NA' OR
	x = 'NA' OR
	y = 'NA' OR
	st_x = 'NA' OR
	st_y = 'NA';
	
COMMIT; -- Save Changes

-- Change the datatype of team_id_against from character to smalltint:
ALTER TABLE game_plays
ALTER COLUMN team_id_against TYPE smallint
USING team_id_against::smallint;

-- Change the datatype of column x from character to smalltint:
ALTER TABLE game_plays
ALTER COLUMN x TYPE smallint
USING x::smallint;

-- Change the datatype of column y from character to smalltint:
ALTER TABLE game_plays
ALTER COLUMN y TYPE smallint
USING y::smallint;

-- Change the datatype of periodtimeremaining from character to smalltint:
ALTER TABLE game_plays
ALTER COLUMN periodtimeremaining TYPE smallint
USING periodtimeremaining::smallint;

-- Change the datatype of st_x from character to smalltint:
ALTER TABLE game_plays
ALTER COLUMN st_x TYPE smallint
USING st_x::smallint;

-- Change the datatype of st_y from character to smalltint:
ALTER TABLE game_plays
ALTER COLUMN st_y TYPE smallint
USING st_y::smallint;

/*
The datetime column is currently imported as a character type. The following steps are
taken to convert this column to separate date and time columns:
1 - Extract date into a separate column called event_date, and time into a column called event_time
Example: 2010-10-08 01:01:11 is separated into 2010-10-08 and 01:01
*/
-- Create a temporary table with the new columns:
CREATE TABLE game_plays_temp AS
SELECT *,
SUBSTRING(datetime, 1, 10) AS event_date,
SUBSTRING(datetime, 12, 5) AS event_time
FROM game_plays;

-- View the first 10 rows of the new table:
SELECT * 
FROM game_plays_temp
LIMIT 10;

-- Change the datatype of event_date column from character to date:
ALTER TABLE game_plays_temp
ALTER COLUMN event_date TYPE DATE USING TO_DATE(event_date, 'YYYY-MM-DD');

-- Change the datatype of event_time column from character to time:
ALTER TABLE game_plays_temp
ALTER COLUMN event_time TYPE TIME USING event_time::TIME;

-- View the first 10 rows of the new table and crosscheck the all column datatypes:
SELECT * 
FROM game_plays_temp
LIMIT 10;

-- Drop the game_plays table
DROP TABLE game_plays;

COMMIT; -- Save Changes

-- Create a new table game_plays with columns in the desired order and leaving out the datetime column:
CREATE TABLE game_plays AS
SELECT
play_id,
game_id,
team_id_for,
team_id_against,
event,
secondaryType,
x,
y,
period,
periodType,
periodTime,
periodTimeRemaining,
event_date,
event_time,
goals_away,
goals_home,
description,
st_x,
st_y
FROM game_plays_temp;

-- View the first 10 rows of game_plays table and crosscheck column positions:
SELECT * 
FROM game_plays
LIMIT 10;

-- Drop the game_plays_temp table
DROP TABLE game_plays_temp;

COMMIT; --Save Changes

-- Export the cleaned game_plays table to a system folder:
COPY 
(
	SELECT * 
	FROM game_plays
	ORDER BY game_id
) 
TO 'D:/E-Resource/Data_and_AI/NHL_Game_Data/clean_data/game_plays_cleaned.csv' WITH CSV HEADER;
-- 4217063 Copied

-- **************************************************************************************************** --

-- Create the game_goalie_stats table:

CREATE TABLE public.game_goalie_stats
(
    game_id integer,
    player_id integer,
    team_id smallint,
    timeOnIce smallint,
    assists smallint,
    goals smallint,
    pim smallint,
    shots smallint,
    saves smallint,
    powerPlaySaves smallint,
    shortHandedSaves smallint,
    evenSaves smallint,
    shortHandedShotsAgainst smallint,
    evenShotsAgainst smallint,
    powerPlayShotsAgainst smallint,
    decision character varying,
    savePercentage character varying, -- imported as character due to the presence of NAs
    powerPlaySavePercentage character varying, -- imported as character due to the presence of NAs
    evenStrengthSavePercentage character varying -- imported as character due to the presence of NAs
)
TABLESPACE pg_default;

ALTER TABLE public.game_goalie_stats
    OWNER to postgres;
	
-- Add a foreign key constraint to game_goalie_stats table:
ALTER TABLE game_goalie_stats
ADD CONSTRAINT game_id_Fkey 
FOREIGN KEY (game_id)
REFERENCES game (game_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;

-- Import game_goalie_stats data from game_goalie_stats.csv into the table:
COPY game_goalie_stats FROM 'D:/E-Resource/Data_and_AI/NHL_Game_Data/game_goalie_stats.csv' WITH CSV HEADER DELIMITER ',';
-- 56656 Records Copied

-- Observe the first 10 rows of the table:
SELECT * 
FROM game_goalie_stats
LIMIT 10;

-- Set all empty fields in the decision column to nulls:
UPDATE game_goalie_stats
SET decision = NULL
WHERE decision = '';
-- 4062 Updated

-- Set all NAs in savePercentage column to nulls:
UPDATE game_goalie_stats
SET savePercentage = NULL
WHERE savePercentage = 'NA';
-- 139 Updated

-- Set all NAs in powerPlaySavePercentage column to nulls:
UPDATE game_goalie_stats
SET powerPlaySavePercentage = NULL
WHERE powerPlaySavePercentage = 'NA';
-- 4743 Updated

-- Set all NAs in evenStrengthSavePercentage column to nulls:
UPDATE game_goalie_stats
SET evenStrengthSavePercentage = NULL
WHERE evenStrengthSavePercentage = 'NA';
-- 197 Updated

COMMIT; -- Save Changes

SELECT savepercentage, ROUND(savepercentage::DECIMAL, 2)
FROM game_goalie_stats
LIMIT 10;

-- Change the datatype of savePercentage from character to numeric with 2 decimal precision:
ALTER TABLE game_goalie_stats
ALTER COLUMN savePercentage TYPE NUMERIC
USING ROUND(savepercentage::NUMERIC, 2);

-- Change the datatype of powerPlaySavePercentage from character to numeric with 2 decimal precision:
ALTER TABLE game_goalie_stats
ALTER COLUMN powerPlaySavePercentage TYPE NUMERIC
USING ROUND(powerPlaySavePercentage::NUMERIC, 2);

-- Change the datatype of evenStrengthSavePercentage from character to numeric with 2 decimal precision:
ALTER TABLE game_goalie_stats
ALTER COLUMN evenStrengthSavePercentage TYPE NUMERIC
USING ROUND(evenStrengthSavePercentage::NUMERIC, 2);

COMMIT; -- Save changes

-- Observe the first 10 rows and column datatypes of the table:
SELECT * 
FROM game_goalie_stats
LIMIT 10;

-- Export the cleaned game_goalie_stats table to a system folder:
COPY 
(
	SELECT * 
	FROM game_goalie_stats
	ORDER BY game_id
) 
TO 'D:/E-Resource/Data_and_AI/NHL_Game_Data/clean_data/game_goalie_stats_cleaned.csv' WITH CSV HEADER;
-- 56656 Copied	

-- **************************************************************************************************** --

/*
The player_info dataset has two columns for player heights - one in imperial 
units and the other in metric. Imperial height column is deleted to reduce 
dimensionality and for simplicity. height_cm column is renamed to height. This is in spreadsheet.
*/
-- Create the player_info table:
CREATE TABLE public.player_info
(
    player_id integer,
    firstName character varying,
    lastName character varying,
    nationality character varying,
    birthCity character varying,
    primaryPosition character varying,
    "birthDate" character varying, -- this is imported as a character type for ease of import. To be converted afterwards.
    birthStateProvince character varying,
    height character varying, -- this is imported as a character type due to the presence of NA. To be converted afterwards.
    weight character varying, -- this is imported as a character type due to the presence of NA. To be converted afterwards.
    shootsCatches character varying,
    PRIMARY KEY (player_id)
)
TABLESPACE pg_default;

ALTER TABLE public.player_info
    OWNER to postgres;
	
-- Import player_info data from player_info.csv into the newly created player_info table:
COPY player_info FROM 'D:/E-Resource/Data_and_AI/NHL_Game_Data/player_info.csv' WITH CSV HEADER DELIMITER ',';
-- 3925 Records Copied

-- Change the datatype of the birthdate column from character to date:
ALTER TABLE player_info
ALTER COLUMN birthdate TYPE DATE
USING TO_DATE(birthdate, 'MM-DD-YYYY')

/*
The following players - scott foster, ben wexler, josef korenar, kyle keyser, 
mason marchment, jacob macdonald, kaden fulcher, and niclas westerholm have missing data 
for either or all of the following fields:
nationality, birthcity, height, and weight. Other missing fields are left as NAs.
An internet search is done to find and update the missing data
References:
https://www.hockey-reference.com/players
https://www.nhl.com/player
https://en.wikipedia.org/wiki/Niclas_Westerholm
*/
UPDATE player_info
SET nationality = 'CAN',
	birthcity = 'Sarnia',
	height = '183.00',
	weight = '185.00'
WHERE firstname = 'Scott' AND lastname = 'Foster' AND player_id = 8479138;

UPDATE player_info
SET nationality = 'CZE'
WHERE firstname = 'Josef' AND lastname = 'Korenar' AND player_id = 8480373;

UPDATE player_info
SET nationality = 'CAN'
WHERE firstname = 'Mason' AND lastname = 'Marchment' AND player_id = 8478975;

UPDATE player_info
SET nationality = 'USA'
WHERE firstname = 'Jacob' AND lastname = 'MacDonald' AND player_id = 8479439;

UPDATE player_info
SET nationality = 'CAN'
WHERE firstname = 'Kaden' AND lastname = 'Fulcher' AND player_id = 8480363;

UPDATE player_info
SET nationality = 'USA',
	birthcity = 'Palo Alto',
	birthstateprovince = 'CA',
	height = '178.00',
	weight = '189.00'
WHERE firstname = 'Ben' AND lastname = 'Wexler' AND player_id = 8480718;

UPDATE player_info
SET nationality = 'USA',
	height = '188.00',
	weight = '183.00'
WHERE firstname = 'Kyle' AND lastname = 'Keyser' AND player_id = 8480356;

UPDATE player_info
SET nationality = 'FIN'
WHERE firstname = 'Niclas' AND lastname = 'Westerholm' AND player_id = 8480779;

COMMIT; -- Save Changes

-- Change the datatype for the height and weight columns from character to numeric:
ALTER TABLE player_info
ALTER COLUMN height TYPE NUMERIC
USING ROUND(height::NUMERIC, 2)

ALTER TABLE player_info
ALTER COLUMN weight TYPE NUMERIC
USING ROUND(weight::NUMERIC, 2)

-- View the first 10 records and crosschaeck the datatypes for each column:
SELECT *
FROM player_info
LIMIT 10;

-- Export the cleaned player_info table to a system folder:
COPY 
(
	SELECT * 
	FROM player_info
	ORDER BY firstname, lastname
) 
TO 'D:/E-Resource/Data_and_AI/NHL_Game_Data/clean_data/player_info.csv' WITH CSV HEADER;
-- 3925 Copied	

-- **************************************************************************************************** --

-- Create the team_info table:
CREATE TABLE public.team_info
(
    team_id smallint,
    franchise_id smallint,
    shortname character varying,
    teamname character varying,
    abbreviation character(3),
    link character varying,
    PRIMARY KEY (team_id)
)
TABLESPACE pg_default;

ALTER TABLE public.team_info
    OWNER to postgres;
	

-- Import team_info data from team_info.csv into the newly created team_info table:
COPY team_info FROM 'D:/E-Resource/Data_and_AI/NHL_Game_Data/team_info.csv' WITH CSV HEADER DELIMITER ',';
-- 33 Records Copied

SELECT * 
FROM team_info;

-- Create a foreign key constraint on game_goalie_stats table to reference the newly create team_info table:
ALTER TABLE game_goalie_stats
ADD CONSTRAINT team_id_Fkey FOREIGN KEY (team_id)
REFERENCES team_info (team_id)
ON DELETE RESTRICT
ON UPDATE RESTRICT;
/*
The above query fails with the following error:
ERROR:  insert or update on table "game_goalie_stats" violates foreign key constraint "team_id_fkey"
DETAIL:  Key (team_id)=(88) is not present in table "team_info".
*/

-- The query below is used to pull out the team_ids in the game_goalie_stats table not present in team_info table:
SELECT DISTINCT ggs.team_id AS ggs_team_id
FROM game_goalie_stats ggs
LEFT JOIN team_info ti
ON ggs.team_id = ti.team_id
WHERE ti.team_id is null;
-- 4 records returned: team_ids 87, 88, 89 and 90

-- Details for the players, team_id, and games are show below. These team_id will be inserted into the team_info table:
WITH misn_teams AS
(
SELECT DISTINCT ggs.team_id AS ggs_team_id
FROM game_goalie_stats ggs
LEFT JOIN team_info ti
ON ggs.team_id = ti.team_id
WHERE ti.team_id is null
)
SELECT
	DISTINCT ggs.player_id,
	pi.firstname,
	pi.lastname,
	ggs.team_id,
	g.match_date,
	g.season,
	g.venue
FROM game_goalie_stats ggs
JOIN player_info pi ON ggs.player_id = pi.player_id
JOIN game g ON ggs.game_id = g.game_id
WHERE ggs.team_id IN (SELECT * FROM misn_teams)
ORDER BY g.venue;

SELECT *
FROM team_info;

INSERT INTO team_info (team_id, franchise_id, shortname, teamname, abbreviation, link)
VALUES 
	(87, null, 'NA', 'NA', 'NA', 'NA'),
	(88, null, 'NA', 'NA', 'NA', 'NA'),
	(89, null, 'NA', 'NA', 'NA', 'NA'),
	(90, null, 'NA', 'NA', 'NA', 'NA');
	
COMMIT; -- Save Changes
-- We can now retry the previous query to create a foriegn key constraint
-- Create a foreign key constraint on game_goalie_stats table to reference the newly create team_info table:
ALTER TABLE game_goalie_stats
ADD CONSTRAINT team_id_Fkey FOREIGN KEY (team_id)
REFERENCES team_info (team_id)
ON DELETE RESTRICT
ON UPDATE RESTRICT;
-- Table Altered

-- **************************************************************************************************** --

-- Create the game_skater_stats table:
CREATE TABLE public.game_skater_stats
(
    game_id integer,
    player_id integer,
    team_id smallint,
    time_on_ice smallint,	
    assists smallint,
    goals smallint,
    shots smallint,
    hits character varying,  -- this is imported as a character type due to the presence of NA. To be converted afterwards.
    power_play_goals smallint,
    power_play_assists smallint,
    penalty_minutes smallint,
    face_off_wins smallint,
    face_off_taken smallint,
    takeaways character varying,  -- this is imported as a character type due to the presence of NA. To be converted afterwards.
    giveaways character varying,  -- this is imported as a character type due to the presence of NA. To be converted afterwards.
    short_handed_goals smallint,
    short_handed_assists smallint,
    blocked character varying,  -- this is imported as a character type due to the presence of NA. To be converted afterwards.
    plus_minus smallint,
    even_time_on_ice smallint,
    short_handed_time_on_ice smallint,
    power_play_time_on_ice smallint
)
TABLESPACE pg_default;

ALTER TABLE public.game_skater_stats
    OWNER to postgres;
	
-- Import game_skater_stats data from game_skater_stats.csv into the newly created game_skater_stats table:
COPY game_skater_stats FROM 'D:/E-Resource/Data_and_AI/NHL_Game_Data/game_skater_stats.csv' WITH CSV HEADER DELIMITER ',';
-- 945830 Records Copied

-- View the first 10 rows of game_skater_stats:
SELECT *
FROM game_skater_stats
LIMIT 10;

-- Change all occurences of NAs to NULLs:
UPDATE game_skater_stats
SET hits = NULL,
	takeaways = NULL,
	giveaways = NULL,
	blocked = NULL
WHERE hits = 'NA' OR takeaways = 'NA' OR giveaways = 'NA' OR blocked = 'NA';
-- 398107 Updated

-- Confirm the absence of NAs in the columns:
SELECT COUNT(*)
FROM game_skater_stats
WHERE hits = 'NA' OR takeaways = 'NA' OR giveaways = 'NA' OR blocked = 'NA';
-- 0 Records returned

-- Change the datatype of the columns from character to smallint:
ALTER TABLE game_skater_stats
ALTER COLUMN hits TYPE smallint USING hits::smallint;

ALTER TABLE game_skater_stats
ALTER COLUMN takeaways TYPE smallint USING takeaways::smallint;

ALTER TABLE game_skater_stats
ALTER COLUMN giveaways TYPE smallint USING giveaways::smallint;

ALTER TABLE game_skater_stats
ALTER COLUMN blocked TYPE smallint USING blocked::smallint;

COMMIT; -- Save Changes

-- View the first 10 rows of game_skater_stats and crosscheck all column datatypes:
SELECT *
FROM game_skater_stats
LIMIT 10;

-- **************************************************************************************************** --

-- Create the game_teams_stats table:
CREATE TABLE public.game_teams_stats
(
    game_id integer,
    team_id smallint,
    hoa character varying,
    won character varying,
    settled_in character varying,
    head_coach character varying,
    goals character varying,
    shots character varying,
    hits character varying,
    pim character varying,
    power_play_opportunities character varying,
    power_play_goals character varying,
    face_off_win_percentage character varying,
    giveaways character varying,
    takeaways character varying,
    blocked character varying,
    start_rink_side character varying
)
TABLESPACE pg_default;

ALTER TABLE public.game_teams_stats
    OWNER to postgres;


-- Import game_teams_stats data from game_teams_stats.csv into the newly created game_teams_stats table:
COPY game_teams_stats FROM 'D:/E-Resource/Data_and_AI/NHL_Game_Data/game_teams_stats.csv' WITH CSV HEADER DELIMITER ',';
-- 52610 Records Copied

-- View the first 10 rows of game_teams_stats:
SELECT *
FROM game_teams_stats
LIMIT 10;

-- Change all occurences of NAs to NULLs:
UPDATE game_teams_stats
SET goals = NULL,
	shots = NULL,
	hits = NULL,
	pim = NULL,
	power_play_opportunities = NULL,
	power_play_goals = NULL,
	face_off_win_percentage = NULL,
	giveaways = NULL,
	takeaways = NULL,
	blocked = NULL
WHERE 
	goals = 'NA' OR 
	shots = 'NA' OR 
	hits = 'NA' OR 
	pim = 'NA' OR
	power_play_opportunities = 'NA' OR
	power_play_goals = 'NA' OR
	face_off_win_percentage = 'NA' OR
	giveaways = 'NA' OR
	takeaways = 'NA' OR
	blocked = 'NA';
-- 22148 Updated

-- Change the datatype of the columns from character to smallint:
ALTER TABLE game_teams_stats
ALTER COLUMN goals TYPE smallint USING goals::smallint;

ALTER TABLE game_teams_stats
ALTER COLUMN shots TYPE smallint USING shots::smallint;

ALTER TABLE game_teams_stats
ALTER COLUMN hits TYPE smallint USING hits::smallint;

ALTER TABLE game_teams_stats
ALTER COLUMN pim TYPE smallint USING pim::smallint;

ALTER TABLE game_teams_stats
ALTER COLUMN power_play_opportunities TYPE smallint USING power_play_opportunities::smallint;

ALTER TABLE game_teams_stats
ALTER COLUMN power_play_goals TYPE smallint USING power_play_goals::smallint;

ALTER TABLE game_teams_stats
ALTER COLUMN goals TYPE smallint USING goals::smallint;

ALTER TABLE game_teams_stats
ALTER COLUMN face_off_win_percentage TYPE NUMERIC
USING ROUND(face_off_win_percentage::NUMERIC, 2);

ALTER TABLE game_teams_stats
ALTER COLUMN giveaways TYPE smallint USING giveaways::smallint;

ALTER TABLE game_teams_stats
ALTER COLUMN takeaways TYPE smallint USING takeaways::smallint;

ALTER TABLE game_teams_stats
ALTER COLUMN blocked TYPE smallint USING blocked::smallint;

-- View the first 10 rows of game_teams_stats and crosscheck all column datatypes:
SELECT *
FROM game_teams_stats
LIMIT 10;

COMMIT; -- Save Changes

SELECT *
FROM information_schema.table_constraints
LIMIT 10;

-- Check for current constraints on the tables:
SELECT table_name, constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name IN ('game', 'team_info', 'game_plays', 'game_goalie_stats', 'player_info', 'game_skater_stats', 'game_teams_stats')
ORDER BY table_name;

-- Add additional constraints:
ALTER TABLE game_plays
ADD CONSTRAINT game_id_Fkey FOREIGN KEY (game_id)
REFERENCES game (game_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;	

ALTER TABLE game_plays
ADD CONSTRAINT team_id_for_Fkey FOREIGN KEY (team_id_for)
REFERENCES team_info (team_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;	

ALTER TABLE game_plays
ADD CONSTRAINT team_id_against_Fkey FOREIGN KEY (team_id_against)
REFERENCES team_info (team_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;	

ALTER TABLE game_skater_stats
ADD CONSTRAINT game_id_Fkey FOREIGN KEY (game_id)
REFERENCES game (game_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;

ALTER TABLE game_teams_stats
ADD CONSTRAINT game_id_Fkey FOREIGN KEY (game_id)
REFERENCES game (game_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;

ALTER TABLE game_skater_stats
ADD CONSTRAINT player_id_Fkey FOREIGN KEY (player_id)
REFERENCES player_info (player_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;

ALTER TABLE game_skater_stats
ADD CONSTRAINT team_id_Fkey FOREIGN KEY (team_id)
REFERENCES team_info (team_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;

ALTER TABLE game_goalie_stats
ADD CONSTRAINT player_id_Fkey FOREIGN KEY (player_id)
REFERENCES player_info (player_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;

ALTER TABLE game_teams_stats
ADD CONSTRAINT team_id_Fkey FOREIGN KEY (team_id)
REFERENCES team_info (team_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;


	
SELECT *
FROM game_skater_stats
WHERE team_id IN 
(
SELECT team_id
FROM team_info
WHERE shortname = 'Winnipeg' AND teamname = 'Jets'
);

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

-- 
SELECT *
FROM player_info
WHERE player_id IN 
(
	SELECT player_id
	FROM game_goalie_stats
);

SELECT *
FROM game_goalie_stats;

WITH goalie_details AS
(
	SELECT 
		DISTINCT ggs.player_id,
		ggs.game_id,
		CONCAT(pi.firstname, ' ', pi.lastname) player_name,
		pi.nationality,
		ROUND(pi.height * 0.01, 2) AS height_m,
		ROUND(pi.weight * 0.45, 2) AS weight_kg,
		ROUND((pi.weight * 0.45) / ((pi.height * 0.01)^2), 2) AS "BMI", -- Player Body-Mass-Index calculation
		CONCAT(ti.shortname, ' ', ti.teamname) AS team_name,
		ggs.timeonice,
		ggs.powerplaysaves
	FROM game_goalie_stats ggs
	JOIN player_info pi
	ON ggs.player_id = pi.player_id
	JOIN team_info ti
	ON ggs.team_id = ti.team_id
	WHERE ti.shortname IN ('Winnipeg', 'Vancouver', 'Ottawa', 'Montreal', 'Toronto', 'Calgary', 'Edmonton')
)
SELECT 
	player_id,
	player_name,
	COUNT(player_id) AS No_of_appearance,
	ROUND(AVG(timeonice), 2) AS "avg_timeOnIce"
FROM goalie_details
GROUP BY player_id, player_name;
	
WITH goalie_performance AS
(
	SELECT 
		pi.player_id AS id,
		CONCAT(pi.firstname, ' ', pi.lastname) AS goalie_name,
		ROUND((pi.weight * 0.45) / ((pi.height * 0.01)^2), 2) AS "BMI", -- Player Body-Mass-Index calculation,
		ROUND(AVG(ggs.timeonice), 2) AS "avg_timeOnIce",
		ROUND(AVG(saves), 2) AS avg_saves,
		ROUND(AVG(powerplaysaves), 2) AS "avg_powerPlaySaves"
	FROM player_info pi
	JOIN game_goalie_stats ggs
	ON pi.player_id = ggs.player_id
	GROUP BY id, goalie_name, "BMI"
	ORDER BY id
),
can_team_goalie AS
(
	SELECT 
		ggs.player_id AS goalie_id,
		ti.team_id
	FROM game_goalie_stats ggs
	JOIN team_info ti
	ON ggs.team_id = ti.team_id
	WHERE ti.shortname IN ('Winnipeg', 'Vancouver', 'Ottawa', 'Montreal', 'Toronto', 'Calgary', 'Edmonton')
	ORDER BY player_id
)
SELECT *
FROM goalie_performance
WHERE id IN 
(
	SELECT DISTINCT goalie_id
	FROM can_team_goalie
)
ORDER BY avg_saves DESC
LIMIT 10;

	
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

	


 


















