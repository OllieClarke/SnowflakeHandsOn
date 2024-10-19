use database ags_game_audience;
use schema raw;

//turn off the tasks
alter task cdc_load_logs_enhanced suspend;
alter task get_new_files suspend;
alter task load_logs_enhanced suspend;

//pause the pipe
alter pipe pipe_get_new_files set pipe_execution_paused = true;


//make a curated schema
create or replace schema curated;

//love listagg()
select gamer_name
      , listagg(game_event_ltz,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;

//window functions too
select gamer_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by gamer_name 
                    order by game_event_ltz
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;

//heatmap sql
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF)
where logout is not null;
