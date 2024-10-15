use database mels_smoothie_challenge_db;
use schema trails;

//we can put a normal view or an external table on top of a stage, but not a materialised view

//try to make an external table
create or replace external table t_cherry_creek_trail(
    my_filename varchar(100) as (metadata$filename::varchar(100))
)
location = @trails_parquet
auto_refresh = true
file_format = (type =  parquet)
;

//fails as our stage is internal, not external
//connect to an external stage
CREATE STAGE EXTERNAL_AWS_DLKW 
	URL = 's3://uni-dlkw' 
	DIRECTORY = ( ENABLE = true );

//use this external stage
create or replace external table t_cherry_creek_trail(
    my_filename varchar(100) as (metadata$filename::varchar(100))
)
location = @external_aws_dlkw
auto_refresh = true
file_format = (type =  parquet)
;

//test
select * from t_cherry_creek_trail;

//you can put a materialised view on top of an external table though
create or replace secure materialized view SMV_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR,
    DISTANCE_TO_MELANIES
) as (select 
value:sequence_1::number as point_id
,value:trail_name::varchar as trail_name
,value:latitude::number(11,8) as lng
,value:longitude::number(11,8)  as lat
,lng||' '||lat as coord_pair
,locations.distance_to_MC(lng,lat) as distance_to_melanies
from T_CHERRY_CREEK_TRAIL)
;

//test
select * from smv_cherry_creek_trail;


//Let's look at Apache Icebergs
use role accountadmin;

//Make an external volume linked to their s3
CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'iceberg-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://uni-dlkw-iceberg'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::321463406630:role/dlkw_iceberg_role'
            STORAGE_AWS_EXTERNAL_ID = 'dlkw_iceberg_id'
         )
      );

//check it worked
desc external volume iceberg_external_volume;
