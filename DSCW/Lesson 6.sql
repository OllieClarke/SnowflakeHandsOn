-- make a new schema
use role sysadmin;
create or replace schema camillas_db.classification;

-- create table (copied from lesson)
create or replace table camillas_db.classification.train_player_position (
	player_id number(38,0),
	position_code varchar(1),
	game number(38,0),
	minutes_played number(38,0),
	goals number(38,0),
	assists number(38,0),
	shots number(38,0),
	passes number(38,0),
	sprint_distance number(38,0),
	saves number(38,0),
	dribbles number(38,0),
	blocks number(38,0),
	claims number(38,0)
);


COPY INTO "CAMILLAS_DB"."CLASSIFICATION"."TRAIN_PLAYER_POSITION"
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
    FROM '@"CAMILLAS_DB"."CORTEX_ANALYST"."CORTEX_ANALYST_MODEL_STAGE"'
)
FILES = ('train_player_positions.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=TRUE,
    FIELD_OPTIONALLY_ENCLOSED_BY='"',
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
)
ON_ERROR=ABORT_STATEMENT;
-- For more details, see: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

-- make table
create or replace table camillas_db.classification.unclassified_player_positions (
	player_id number(38,0),
	game_id number(38,0),
	mins_played number(38,0),
	goals_made number(38,0),
	assists number(38,0),
	shots number(38,0),
	passes number(38,0),
	sprint_distance number(38,0),
	saves number(38,0),
	dribbles number(38,0),
	blocks number(38,0),
	claims number(38,0)
);

COPY INTO camillas_db.classification.unclassified_player_positions
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
    FROM '@"CAMILLAS_DB"."CORTEX_ANALYST"."CORTEX_ANALYST_MODEL_STAGE"'
)
FILES = ('unclassified_player_data.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=TRUE,
    FIELD_OPTIONALLY_ENCLOSED_BY='"',
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
)
ON_ERROR=ABORT_STATEMENT;



-- Make a model

-- This was my attempt without a wizard:
-- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role SYSADMIN;
use warehouse ML_WH;
use database CAMILLAS_DB;
use schema CLASSIFICATION;


-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE SNOWFLAKE.ML.CLASSIFICATION player_position_classification_OC(
    INPUT_DATA => SYSTEM$REFERENCE('table', 'train_player_position'),
    TARGET_COLNAME => 'POSITION_CODE'
);

-- Generate predictions and store the results to a table.
BEGIN
    -- This is the step that creates your predictions.
    SELECT *, player_position_classification_OC!PREDICT(INPUT_DATA => {*}) 
        as predictions 
    from unclassified_player_positions;
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE TABLE my_player_pos_code_classifs_OC AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View your predictions.
SELECT * FROM my_player_pos_code_classifs_OC;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of your model. 
CALL player_position_classification_OC!SHOW_EVALUATION_METRICS();


-- this is the wizard's code:

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION player_position_classification(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'TRAIN_PLAYER_POSITION'),
    TARGET_COLNAME => 'POSITION_CODE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Inspect your logs to ensure training completed successfully. 
CALL player_position_classification!SHOW_TRAINING_LOGS();

-- Generate predictions as new columns in to your prediction table.
CREATE TABLE my_player_pos_code_classifs AS SELECT
    *, 
    player_position_classification!PREDICT(
        OBJECT_CONSTRUCT(*),
        -- This option alows the prediction process to complete even if individual rows must be skipped.
        {'ON_ERROR': 'SKIP'}
    ) as predictions
from UNCLASSIFIED_PLAYER_POSITIONS;

-- View your predictions.
SELECT * FROM my_player_pos_code_classifs;

-- Parse the prediction results into separate columns. 
-- Note: This is a just an example. Be sure to update this to reflect 
-- the classes in your dataset.
SELECT * EXCLUDE predictions,
        predictions:class AS position_code,
        round(predictions['probability'][position_code], 3) as probability
FROM my_player_pos_code_classifs;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect your model's evaluation metrics.
CALL player_position_classification!SHOW_EVALUATION_METRICS();
CALL player_position_classification!SHOW_GLOBAL_EVALUATION_METRICS();
CALL player_position_classification!SHOW_CONFUSION_MATRIX();

-- Inspect the relative importance of your features, including auto-generated features.  
CALL player_position_classification!SHOW_FEATURE_IMPORTANCE();
