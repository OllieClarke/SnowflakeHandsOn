//where is my code running?
select current_timestamp(); --in Cali

//change session to use utc
alter session set timezone='UTC';

//now it's different
select current_timestamp();

//change to shanghai
alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

//and to where I am
alter session set timezone = 'Europe/London';
select current_timestamp();

//see all parameters we can change
show parameters;

use database ags_game_audience;
use schema raw;

select * from logs;

//look at the updated data
select $1
from @uni_kishore/updated_feed
(file_format => ff_json_logs);

//load the new data in
copy into game_logs
from @uni_kishore/updated_feed
file_format = (format_name = ff_json_logs);

//all the data
select * from logs;

//just the new data
select * from logs
where agent is null;

//update the view to remove agent and include ip_address
create or replace view logs as
(select
raw_log:ip_address::text as ip_address
,raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
,raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log
from game_logs
where ip_address is not null);

//check
select * from logs;

//find the calls?
select 
*
from logs
where user_login LIKE '%prajina%';

//datetime is in UTC not LTZ
