//create a new database called INTL_DB
use role sysadmin;

create database intl_db;

use schema intl_db.public;

//Create a warehouse for loading intl_db
use role sysadmin;

create warehouse intl_wh
with
warehouse_size = 'XSMALL'
warehouse_type = 'STANDARD'
auto_suspend = 600 --600 seconds/10 mins
auto_resume = TRUE;

use warehouse intl_wh;

//Create table intl_stds_org_3166
create or replace table intl_db.public.INT_STDS_ORG_3166 
(iso_country_name varchar(100), 
 country_name_official varchar(200), 
 sovreignty varchar(40), 
 alpha_code_2digit varchar(2), 
 alpha_code_3digit varchar(3), 
 numeric_country_code integer,
 iso_subdivision varchar(15), 
 internet_domain_code varchar(10)
);

//Create a fileformat of csv for loading data
create or replace file format util_db.public.PIPE_DBLQUOTE_HEADER_CR 
  type = 'CSV' --use CSV for any flat file
  compression = 'AUTO' 
  field_delimiter = '|' --pipe or vertical bar
  record_delimiter = '\r' --carriage return
  skip_header = 1  --1 header row
  field_optionally_enclosed_by = '\042'  --double quotes
  trim_space = FALSE;


//check existing stages
show stages in account;
//none, which is odd

//change user then see if there are any
use role accountadmin;
show stages in account;
//there they are

//back to tutorial role
use role sysadmin;

//Create a stage
create stage util_db.public.aws_s3_bucket url ='s3://uni-cmcw';

//see files in stage
list @util_db.public.aws_s3_bucket;

//copy data into created table
copy into INTL_DB.PUBLIC.INT_STDS_ORG_3166
from @util_db.public.aws_s3_bucket
files = ( 'ISO_Countries_UTF8_pipe.csv' )
file_format = ( format_name='UTIL_DB.PUBLIC.PIPE_DBLQUOTE_HEADER_CR');

//did it work?
select count(*) as found,
'249' as expected
from INTL_DB.PUBLIC.INT_STDS_ORG_3166;
//nice

//let's use the information schema to see if the table is there
select count(*) as objects_found
from intl_db.information_schema.tables
where table_schema='PUBLIC'
and table_name = 'INT_STDS_ORG_3166';


//now lets use it to see how many rows there are
select row_count
from intl_db.information_schema.tables
where table_schema='PUBLIC'
and table_name = 'INT_STDS_ORG_3166';


//right, back to our data
--join local data with shared data
select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey;

//lets make this a view
create view intl_db.public.nations_sample_plus_iso
(
iso_country_name
,country_name_official
,alpha_code_2digit
,region
) AS
select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey;

//look at the view
select * from intl_db.public.nations_sample_plus_iso;


//create tables
create table intl_db.public.CURRENCIES 
(
  currency_ID integer, 
  currency_char_code varchar(3), 
  currency_symbol varchar(4), 
  currency_digital_code varchar(3), 
  currency_digital_name varchar(30)
)
  comment = 'Information about currencies including character codes, symbols, digital codes, etc.';
 
 create table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
  (
    country_char_code varchar(3), 
    country_numeric_code integer, 
    country_name varchar(100), 
    currency_name varchar(100), 
    currency_char_code varchar(3), 
    currency_numeric_code integer
  ) 
  comment = 'Mapping table currencies to countries';

  //create a new file format
   create file format util_db.public.CSV_COMMA_LF_HEADER
  type = 'CSV' 
  field_delimiter = ',' 
  record_delimiter = '\n' -- the n represents a Line Feed character
  skip_header = 1 
;

//copy in the data to currencies
copy into INTL_DB.PUBLIC.currencies
from @util_db.public.aws_s3_bucket
files = ( 'currencies.csv' )
file_format = ( format_name='UTIL_DB.PUBLIC.csv_comma_lf_header');

//copy in the data to country code to currencies
copy into INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE
from @util_db.public.aws_s3_bucket
files = ( 'country_code_to_currency_code.csv' )
file_format = ( format_name='UTIL_DB.PUBLIC.csv_comma_lf_header');


//Creating a simple view
Create or replace view simple_currency as(
SELECT country_char_code as cty_code,
currency_char_code as cur_code
FROM country_code_to_currency_code
);

//test view
select * from simple_currency;
