-- setup stuff
use role sysadmin;
create or replace database camillas_db;
drop schema camillas_db.public;
create schema camillas_db.cortex_analyst;
use schema camillas_db.cortex_analyst;
create or replace stage cortex_analyst_model_stage
encryption = (type = 'snowflake_full')
directory = (enable=true
auto_refresh = true);
create or replace warehouse llm_wh
WITH warehouse_size = XSMALL
auto_suspend = 150;

-- make tables
create or replace table camillas_db.cortex_analyst.camillas_teams
( 
    team_id number,
    team_name varchar(50),
    kit_color varchar(20),
    coach varchar(100),
    emoji_symbol varchar(5)
);

insert into camillas_db.cortex_analyst.camillas_teams
values
(1,'Blue Sky Strikers','cerulean','Stormy McLeod', '💙☁️⚡️'),
(2,'Pitch Blazing Bombers','emerald','Kelly Groen','🌱🔥💣' ),
(3,'Solar Flashing Flares','marigold','Ravi Bahsin', '☀️🔥'),
(4,'Terracotta Tirade','terracotta','Clay Skála', '🪴💪');


create or replace table camillas_db.cortex_analyst.match_locations
(
 location_id number, 
 location_name varchar(50)
);

insert into camillas_db.cortex_analyst.match_locations
values 
(1, 'Main Street Park - Pitch 1'),
(2, 'Main Street Park - Pitch 2'),
(3, 'Central Park - North Pitch'),
(4, 'Central Park - South Pitch');

create or replace table camillas_db.cortex_analyst.match_schedule
( 
    home_team_id number,
    away_team_id number,
    location_id number,
    match_datetime timestamp_ntz  
);

insert into camillas_db.cortex_analyst.match_schedule
values
(1,2,1,'2025-06-07 08:00:00'),
(3,4,2,'2025-06-07 08:00:00'),
(2,3,3,'2025-06-07 12:00:00'),
(1,4,4,'2025-06-07 12:00:00'),
(1,3,1,'2025-06-07 16:00:00'),
(2,4,2,'2025-06-07 16:00:00')
;