//Make smoothies db
Create or replace database smoothies;

//use smoothies and public
use database smoothies;
use schema public;

//Create a table
create or replace table fruit_options(
fruit_id number, fruit_name varchar(25));


//create the file format for the weird file
create or replace file format two_headerrow_pct_delim
type = CSV,
   skip_header = 2,   
   field_delimiter = '%',
   trim_space = TRUE
;

//Look at the staged file
SELECT $1, $2
FROM @my_uploaded_files/fruits_available_for_smoothies.txt
(FILE_FORMAT => two_headerrow_pct_delim);

//copy the data into the table, but reorder the columns
//purge = true removes the file from the stage
copy into fruit_options
from (
SELECT $2 as fruit_id, $1 as fruit_name
FROM @my_uploaded_files/fruits_available_for_smoothies.txt)
FILE_FORMAT = (format_name = two_headerrow_pct_delim)
on_error = abort_statement
purge = true
;
