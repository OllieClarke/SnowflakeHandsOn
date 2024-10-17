use role sysadmin;
use warehouse compute_wh;

//Get everything set up
create or replace database AGS_GAME_AUDIENCE;
use database AGS_GAME_AUDIENCE;
drop schema public;
create or replace schema raw;

//make a game log table
create or replace table game_logs(
raw_log VARIANT);

//make an external stage
create or replace stage uni_kishore 
	URL = 's3://uni-kishore' 
	DIRECTORY = ( ENABLE = true );

//check what's in the stage
list @uni_kishore/kickoff;


//make a file format
create or replace file format ff_json_logs
type = JSON
strip_outer_array = true
;

//look in the stage
select $1
from @uni_kishore/kickoff
(file_format => ff_json_logs);

//load the data into the raw logs table
copy into game_logs
from @uni_kishore/kickoff
file_format = (format_name = ff_json_logs);

//look in the table
select * from game_logs;

//parse it
select
raw_log:agent::text as agent
,raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
,raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log
from game_logs;

//make a view
create or replace view logs as
(select
raw_log:agent::text as agent
,raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
,raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log
from game_logs);

//check it
select * from logs;

