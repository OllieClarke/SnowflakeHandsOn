--create a table with lots of datatypes
create or replace table util_db.public.my_data_types
(
  my_number number
, my_text varchar(10)
, my_bool boolean
, my_float float
, my_date date
, my_timestamp timestamp_tz
, my_variant variant
, my_array array
, my_object object
, my_geography geography
, my_geometry geometry
, my_vector vector(int,16)
);

//make a db and setup the schema
use role sysadmin;
create or replace database ZENAS_ATHLEISURE_DB;
use database ZENAS_ATHLEISURE_DB;
drop schema public;
create or replace schema products;

//create a stage and put all the images in it (done through UI)
//create a stage (client encryption) and put all the metadata in it (done through UI)

