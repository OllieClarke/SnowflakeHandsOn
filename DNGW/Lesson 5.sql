use database ags_game_audience;
use schema raw;

//make a task
create or replace task load_logs_enhanced
warehouse = compute_wh
schedule = '5 minute'
as
select 'hello';

//give sysadmin the ability to execute tasks
use role accountadmin;
grant execute task on account to role sysadmin;

//back to sysadmin
use role sysadmin;

//run the task
execute task load_logs_enhanced;

//look at all
show tasks in account;

//look at one in detail
describe task load_logs_enhanced;

//run the task a few more times
execute task load_logs_enhanced;
execute task load_logs_enhanced;
execute task load_logs_enhanced;
execute task load_logs_enhanced;

//update the task to do what we actually want
create or replace task load_logs_enhanced
warehouse = compute_wh
schedule = '5 minute'
as
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, city
, region
, country
, timezone as gamer_ltz_name
, convert_timezone('UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
, tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN time_of_day_lu tod
ON hour(game_event_ltz) = tod.hour
;

--make a note of how many rows you have in the table
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task LOAD_LOGS_ENHANCED;

--check to see how many rows were added (if any!)
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

//no new rows added so we need to
//update the task to actually do what we actually want
create or replace task load_logs_enhanced
warehouse = compute_wh
schedule = '5 minute'
as
insert into ags_game_audience.enhanced.logs_enhanced
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, city
, region
, country
, timezone as gamer_ltz_name
, convert_timezone('UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
, tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN time_of_day_lu tod
ON hour(game_event_ltz) = tod.hour
;

//put some more data in
execute task load_logs_enhanced;
execute task load_logs_enhanced;
execute task load_logs_enhanced;

//how many rows now?
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

//truncate the table and reload
truncate table ags_game_audience.enhanced.logs_enhanced;
execute task load_logs_enhanced;

//make a clone of our cleaned logs
create table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
clone ags_game_audience.enhanced.LOGS_ENHANCED;


//let's do a merge
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING RAW.LOGS r
ON r.user_login = e.GAMER_NAME
and r.user_event = e.game_event_name
and r.datetime_iso8601 = e.game_event_utc
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey I updated matching rows!';

//did it work
select * from ags_game_audience.enhanced.logs_enhanced;

//truncate the table
truncate table ags_game_audience.enhanced.logs_enhanced;

//proper merge now
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (
    SELECT logs.ip_address
    , logs.user_login as gamer_name
    , logs.user_event as game_event_name
    , logs.datetime_iso8601 as game_event_utc
    , city
    , region
    , country
    , timezone as gamer_ltz_name
    , convert_timezone('UTC',timezone,logs.datetime_iso8601) as game_event_ltz
    , dayname(game_event_ltz) as dow_name
    , tod_name
    from AGS_GAME_AUDIENCE.RAW.LOGS logs
    JOIN IPINFO_GEOLOC.demo.location loc
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN time_of_day_lu tod
    ON hour(game_event_ltz) = tod.hour
) r 
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
insert (
IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME
)
values (
IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) 
;


//now let's make it a task
create or replace task load_logs_enhanced
warehouse = compute_wh
schedule = '5 minute'
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (
    SELECT logs.ip_address
    , logs.user_login as gamer_name
    , logs.user_event as game_event_name
    , logs.datetime_iso8601 as game_event_utc
    , city
    , region
    , country
    , timezone as gamer_ltz_name
    , convert_timezone('UTC',timezone,logs.datetime_iso8601) as game_event_ltz
    , dayname(game_event_ltz) as dow_name
    , tod_name
    from AGS_GAME_AUDIENCE.RAW.LOGS logs
    JOIN IPINFO_GEOLOC.demo.location loc
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN time_of_day_lu tod
    ON hour(game_event_ltz) = tod.hour
) r 
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
insert (
IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME
)
values (
IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) 
;

//run the task
execute task load_logs_enhanced;

--Testing cycle for MERGE. Use these commands to make sure the Merge works as expected

--Write down the number of records in your table 
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the Merge a few times. No new rows should be added at this time 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed 
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event2", "user_login":"fake user"}');

--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select Count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 
