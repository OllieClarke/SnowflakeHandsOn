-- lesson 1
use role accountadmin;
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DSCW01' as step 
   ,( select  iff(count(*)>=5, 5, 0)
     from (
       select model_name
       from SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AISQL_USAGE_HISTORY
       where function_name ilike '%AI_COMPLETE%'
       group by model_name
          )
     ) as actual 
   , 5 as expected 
   ,'Used Different models when exploring Cortex Playground' as description
); 

-- lesson 3
-- Set your worksheet drop lists
--This DORA Check Requires that you RUN two Statements, one right after the other
list @camillas_db.cortex_analyst.cortex_analyst_model_stage;

--the above command puts information into memory that can be accessed using result_scan(last_query_id())
-- If you have to run this check more than once, always run the LIST command immediately prior
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DSCW02' as step
 ,( select IFF(count(*)>0,1,0) 
    from table(result_scan(last_query_id())) 
    where "name" = 'cortex_analyst_model_stage/CAMILLAS_JUNE_TOURNAMENT.yaml') as actual
 , 1 as expected
 ,'Semantic Model Complete' as description
); 

-- lesson 4
-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DSCW03' as step 
   ,( select  round(count(*)/iff(count(*)=0,1,count(*)),0) as tally
      from snowflake.account_usage.query_history
      where query_text like '%CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_forecasting%'
      and execution_status = 'SUCCESS'
     ) as actual 
   , 1 as expected 
   ,'Created Forecast Model' as description
); 

--lesson 5 
-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DSCW04' as step 
   ,( select  round(count(*)/iff(count(*)=0,1,count(*)),0) as tally
      from snowflake.account_usage.query_history
      where query_text like '%CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_4cast%'
      and execution_status = 'SUCCESS'
     ) as actual 
   , 1 as expected 
   ,'Improved Forecast Model' as description
); 

-- Lesson 6
-- Set your worksheet drop lists
--This DORA Check Requires that you RUN two Statements, one right after the other
call camillas_db.classification.player_position_classification!SHOW_FEATURE_IMPORTANCE();

--the above command puts information into memory that can be accessed using result_scan(last_query_id())
-- If you have to run this check more than once, always run the LIST command immediately prior
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DSCW05' as step
 ,( select count(*) from table(result_scan(last_query_id())) where FEATURE in ('PASSES','MINUTES_PLAYED','DRIBBLES','ASSISTS', 'SAVES', 'CLAIMS')
  ) as actual
 , 6 as expected
 ,'Classification Model Complete' as description
); 