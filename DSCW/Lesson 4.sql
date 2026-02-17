--setup
use role sysadmin;
create or replace schema camillas_db.forecasting;
create or replace warehouse ML_WH
WITH warehouse_size = XSMALL
auto_suspend = 150;

-- make tables
create or replace table camillas_db.forecasting.practice_stats (
	practice_date timestamp_ntz,
	goals_scored number,
	goals_attempted number
);

--answer questions
select count(*)
from practice_stats;

select 
    min(practice_date) as frst
, max(practice_date) as lst
from practice_stats;

-- make a view that uses data from march to july for training the model 
create or replace view camillas_db.forecasting.train_model_practice_data(
	  practice_date,
	  goals_attempted,
	  goals_scored
) as
  select 
    practice_date, 
    goals_attempted,
    goals_scored
  from camillas_db.forecasting.practice_stats
  where practice_date < '2025-07-01';


-- make a view that uses data from july forward for validating the model
create or replace view camillas_db.forecasting.validate_model_practice_data(
	   practice_date,
	   goals_attempted,
	   goals_scored
) as
  select 
    practice_date, 
    goals_attempted,
    goals_scored
  from camillas_db.forecasting.practice_stats
  where practice_date >= '2025-07-01';

  -- code from model wizard

-- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role SYSADMIN;
use warehouse ML_WH;
use database CAMILLAS_DB;
use schema FORECASTING;

-- Inspect the first 10 rows of your training data. This is the data we'll use to create your model.
--commented out as unneeded
--select * from TRAIN_MODEL_PRACTICE_DATA limit 10;

-- Prepare your training data. Timestamp_ntz is a required format.
--commented out as already exists
/*CREATE VIEW TRAIN_MODEL_PRACTICE_DATA_v1 AS SELECT
    * EXCLUDE PRACTICE_DATE,
    to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE_v1
FROM TRAIN_MODEL_PRACTICE_DATA;

-- Prepare your prediction data. Timestamp_ntz is a required format.
CREATE VIEW VALIDATE_MODEL_PRACTICE_DATA_v1 AS SELECT
    * EXCLUDE PRACTICE_DATE,
    to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE_v1
FROM VALIDATE_MODEL_PRACTICE_DATA;
    */

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_forecasting(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'TRAIN_MODEL_PRACTICE_DATA'),
    TIMESTAMP_COLNAME => 'PRACTICE_DATE',
    TARGET_COLNAME => 'GOALS_SCORED'
);

-- Generate predictions and store the results to a table.
BEGIN
    -- This is the step that creates your predictions.
    CALL camillas_practice_goal_forecasting!FORECAST(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'VALIDATE_MODEL_PRACTICE_DATA'),
        TIMESTAMP_COLNAME => 'PRACTICE_DATE',
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE TABLE first_goals_forecast AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View your predictions.
SELECT * FROM first_goals_forecast;

-- Union your predictions with your historical data, then view the results in a chart.
SELECT PRACTICE_DATE, GOALS_SCORED AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM TRAIN_MODEL_PRACTICE_DATA
UNION ALL
SELECT ts as PRACTICE_DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM first_goals_forecast;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of your model. 
CALL camillas_practice_goal_forecasting!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of your features, including auto-generated features. 
CALL camillas_practice_goal_forecasting!EXPLAIN_FEATURE_IMPORTANCE();
