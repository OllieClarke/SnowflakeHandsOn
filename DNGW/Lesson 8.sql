use role sysadmin;
use warehouse compute_wh;
use database ags_game_audience;
use schema raw;

//make a snowpipe
CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

//empty the table
truncate table ags_game_audience.enhanced.logs_enhanced;

//suspend the task
alter task load_logs_enhanced suspend;

//update the load logs enhanced task to use our new table
create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	schedule = '5 minute'
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
    from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS logs
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


//is my pipe running?
select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));

//refresh the pipe if it's stuck
ALTER PIPE ags_game_audience.raw.PIPE_GET_NEW_FILES REFRESH;

//make a stream
create or replace stream ed_cdc_stream
on table ed_pipeline_logs;

//look at it
show streams;

//are there any changes pending?
select system$stream_has_data('ed_cdc_stream');

//suspend all tasks
alter task get_new_files suspend;
alter task load_logs_enhanced suspend;

//query the stream
select * from ed_cdc_stream;

//write a merge statement using the stream rather than looking at the whole table
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (
    SELECT cdc.ip_address
    , cdc.user_login as gamer_name
    , cdc.user_event as game_event_name
    , cdc.datetime_iso8601 as game_event_utc
    , city
    , region
    , country
    , timezone as gamer_ltz_name
    , convert_timezone('UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
    , dayname(game_event_ltz) as dow_name
    , tod_name
    from AGS_GAME_AUDIENCE.RAW.ed_cdc_stream cdc
    JOIN IPINFO_GEOLOC.demo.location loc
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(cdc.ip_address) 
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


//make a final task to pull data from the stream into our output table
create or replace task cdc_load_logs_enhanced
warehouse = compute_wh
schedule = '5 minute'
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (
    SELECT cdc.ip_address
    , cdc.user_login as gamer_name
    , cdc.user_event as game_event_name
    , cdc.datetime_iso8601 as game_event_utc
    , city
    , region
    , country
    , timezone as gamer_ltz_name
    , convert_timezone('UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
    , dayname(game_event_ltz) as dow_name
    , tod_name
    from AGS_GAME_AUDIENCE.RAW.ed_cdc_stream cdc
    JOIN IPINFO_GEOLOC.demo.location loc
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(cdc.ip_address) 
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

//enable the task
alter task cdc_load_logs_enhanced resume;

//suspend the task
alter task cdc_load_logs_enhanced suspend;

/*update the task so it runs every 5 minutes, but only if
there's stuff in the stream */
create or replace task cdc_load_logs_enhanced
warehouse = compute_wh
schedule = '5 minute'
when system$stream_has_data('ed_cdc_stream') --only when there's data
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (
    SELECT cdc.ip_address
    , cdc.user_login as gamer_name
    , cdc.user_event as game_event_name
    , cdc.datetime_iso8601 as game_event_utc
    , city
    , region
    , country
    , timezone as gamer_ltz_name
    , convert_timezone('UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
    , dayname(game_event_ltz) as dow_name
    , tod_name
    from AGS_GAME_AUDIENCE.RAW.ed_cdc_stream cdc
    JOIN IPINFO_GEOLOC.demo.location loc
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(cdc.ip_address) 
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

//enable the task
alter task cdc_load_logs_enhanced resume;

//check data is flowing properly
select count($1) as "count",
'stage' as source
from @uni_kishore_pipeline
union all
select count(*) as "count"
,'ed_pipeline_logs' as source
from ed_pipeline_logs
union all
select count(*) as "count"
, 'logs_enhanced' as source
from ags_game_audience.enhanced.logs_enhanced
;
