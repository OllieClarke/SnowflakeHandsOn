//set everything up
use role sysadmin;
create or replace database mels_smoothie_challenge_db;
use database mels_smoothie_challenge_db;
drop schema public;
create or replace schema trails;
use schema trails;
create or replace stage trails_geojson 
	DIRECTORY = ( ENABLE = true );
create or replace stage trails_parquet
    directory = ( ENABLE = true);
create or replace file format FF_JSON
    type = JSON;
create or replace file format FF_PARQUET
    type = PARQUET;

//look at the geojson stage
select *
from @trails_geojson
(file_format => ff_json);

//look at parquet
select *
from @trails_parquet
(file_format => ff_parquet);
