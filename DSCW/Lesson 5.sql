--overhead
use role sysadmin;
use warehouse ml_wh;
use schema camillas_db.forecasting;

-- make a view with a Day of week column to train on
create or replace view camillas_db.forecasting.train_2_model_practice_data(
	practice_date,
	day_of_week,
	goals_attempted,
	goals_scored
) as
select 
practice_date,
dayname(practice_date) as day_of_week,
goals_attempted,
goals_scored
from camillas_db.forecasting.practice_stats
where practice_date < '2025-07-01';

-- make a view with a day of week column to validate against
create or replace view camillas_db.forecasting.validate_2_model_practice_data(
	   practice_date,
       day_of_week,
	   goals_attempted,
	   goals_scored
) as
  select 
    practice_date, 
    dayname(practice_date) as day_of_week,
    goals_attempted,
    goals_scored
  from camillas_db.forecasting.practice_stats
  where practice_date >= '2025-07-01';


--edited code from before to use the new views:
-- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role SYSADMIN;
use warehouse ML_WH;
use database CAMILLAS_DB;
use schema FORECASTING;


-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_4cast_w_dayofweek(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'TRAIN_2_MODEL_PRACTICE_DATA'),
    SERIES_COLNAME => 'DAY_OF_WEEK',
    TIMESTAMP_COLNAME => 'PRACTICE_DATE',
    TARGET_COLNAME => 'GOALS_SCORED'
);

-- Generate predictions and store the results to a table.
BEGIN
    -- This is the step that creates your predictions.
    CALL camillas_practice_goal_4cast_w_dayofweek!FORECAST(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'VALIDATE_2_MODEL_PRACTICE_DATA'),
        SERIES_COLNAME => 'DAY_OF_WEEK',
        TIMESTAMP_COLNAME => 'PRACTICE_DATE',
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE TABLE second_goals_forecast AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View your predictions.
SELECT * FROM second_goals_forecast;

-- Union your predictions with your historical data, then view the results in a chart.
SELECT PRACTICE_DATE, day_of_week, GOALS_SCORED AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM TRAIN_2_MODEL_PRACTICE_DATA
UNION ALL
SELECT ts as PRACTICE_DATE, series as day_of_week, NULL AS actual, forecast, lower_bound, upper_bound
    FROM second_goals_forecast;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of your model. 
CALL camillas_practice_goal_4cast_w_dayofweek!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of your features, including auto-generated features. 
CALL camillas_practice_goal_4cast_w_dayofweek!EXPLAIN_FEATURE_IMPORTANCE();


-- query from instructions
select practice_date, goals_scored as actual, null as forecast_1, NULL as forecast_2
    from train_2_model_practice_data
UNION ALL
select ts as practice_date, NULL as actual, forecast as forecast_1, NULL as forecast_2
    from first_goals_forecast
UNION ALL    
select ts as practice_date, NULL as actual, null as forecast_1, forecast as forecast_2
    from second_goals_forecast;

-- swapping validation data with forecast 1
select practice_date, goals_scored as actual, NULL as forecast_2
    from train_2_model_practice_data
UNION ALL
select practice_date, goals_scored as actual, NULL as forecast_2
    from validate_2_model_practice_data
UNION ALL    
select ts as practice_date, NULL as actual, forecast as forecast_2
    from second_goals_forecast;