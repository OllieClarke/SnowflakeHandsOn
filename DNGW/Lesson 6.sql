use database ags_game_audience;
use schema raw;

//make a new stage
create or replace stage uni_kishore_pipeline 
	URL = 's3://uni-kishore-pipeline' 
	DIRECTORY = ( ENABLE = true );

list @uni_kishore_pipeline;

//make a new table
create or replace TABLE PL_GAME_LOGS (
	RAW_LOG VARIANT
);

//copy into it
copy into pl_game_logs
from @uni_kishore_pipeline
file_format = (format_name = ff_json_logs);

//what's in the new table?
select COUNT(*) from pl_game_logs;

//make a task to load the log table every 10 minutes
create or replace task get_new_files
warehouse = compute_wh
schedule = '10 minute'
as 
copy into pl_game_logs
from @uni_kishore_pipeline
file_format = (format_name = ff_json_logs);

//run the task
execute task get_new_files;
execute task get_new_files;
execute task get_new_files;
execute task get_new_files;
execute task get_new_files;

select count(*) from pl_game_logs;

//make a pipeline view
create or replace view PL_LOGS(
	IP_ADDRESS,
	DATETIME_ISO8601,
	USER_EVENT,
	USER_LOGIN,
	RAW_LOG
) as
(select
raw_log:ip_address::text as ip_address
,raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
,raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log
from pl_game_logs
where ip_address is not null);

select * from pl_logs;

//update the merge task
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
    from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
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

select count(*) from ags_game_audience.enhanced.logs_enhanced;

//make a resource monitor
use role accountadmin;
create RESOURCE MONITOR IDENTIFIER('"DAILY_SHUT_DOWN"') CREDIT_QUOTA = 1 FREQUENCY = 'DAILY' START_TIMESTAMP = 'IMMEDIATELY' TRIGGERS ON 75 PERCENT DO SUSPEND ON 98 PERCENT DO SUSPEND_IMMEDIATE ON 50 PERCENT DO NOTIFY;
alter ACCOUNT set RESOURCE_MONITOR = 'DAILY_SHUT_DOWN';

//back to sysadmin and truncate table
use role sysadmin;
truncate table ags_game_audience.enhanced.logs_enhanced;

//turn on the tasks
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

select count(*) from ags_game_audience.enhanced.logs_enhanced;

//check everything is working
select count($1) as "count",
'stage' as source
from @uni_kishore_pipeline
union all
select count(*) as "count"
,'pl_game_los' as source
from pl_game_logs
union all
select COUNT(*) as "count"
, 'pl_logs' as source
from pl_logs
union all
select count(*) as "count"
, 'logs_enhanced' as source
from ags_game_audience.enhanced.logs_enhanced
;

//give ourselves servelss access
use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switch back to sysadmin
use role sysadmin;

//update the tasks to use serverless
--USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'

//get new files now serveless and every 5 mins
create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	schedule='5 minute'
	as copy into pl_game_logs
from @uni_kishore_pipeline
file_format = (format_name = ff_json_logs);

//load logs now serverless and after get new files
create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
	after ags_game_audience.raw.get_new_files
	as MERGE INTO ENHANCED.LOGS_ENHANCED e
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
    from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
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
IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME);

//resume the tasks from end to root order
alter task ags_game_audience.raw.load_logs_enhanced resume;
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;

//and suspend the root task
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
