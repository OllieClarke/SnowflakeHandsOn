use database zenas_athleisure_db;
use schema products;

//what files do I have?
list @product_metadata;

//what's the first column of all fiels?
select $1
from @product_metadata;

//what's the first column of the sizes file?
select $1
from @product_metadata/sweatsuit_sizes.txt;

//are they using carats for rows or columns?
//let's look at rows
create file format zmd_file_format_1
RECORD_DELIMITER = '^';

//does this look right?
select $1
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

//let's look at columns
create file format zmd_file_format_2
FIELD_DELIMITER = '^';

//does this look right?
select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

//Let's sort it properly
create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'
trim_space = TRUE;

//much better
select $1, $2
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);


//update ff1 to deal with weirdness
create or replace file format zmd_file_format_1
record_delimiter = ';'
trim_space = TRUE;

//use it
select $1 as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 );

//update ff2 to deal with weirdness
create or replace file format zmd_file_format_2
record_delimiter = ';'
field_delimiter = '|'
trim_space = TRUE;

//use it
select $1 ,$2, $3
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2 );

//crlf looking making things weird, so lets fix
select replace($1,chr(13)||chr(10)) as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 );

//and get rid of null row
select replace($1,chr(13)||chr(10)) as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '';

//make it a view
create or replace view sweatsuit_sizes as
select replace($1,chr(13)||chr(10)) as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '';

select * from sweatsuits_sizes;

//make a sweatband view
create or replace view sweatband_product_line as
select replace($1,chr(13)||chr(10)) as product_code 
, replace($2, chr(13)||chr(10)) as headband_description
, replace($3, chr(13)||chr(10)) as wristband_description
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2 );

select * from sweatband_product_line;

create or replace view sweatband_coordination as
select replace($1, chr(13)||chr(10)) as product_code
, replace($2, chr(13)||chr(10)) as has_matching_sweatsuit
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

select * from sweatband_coordination;

//we good?
select product_code, has_matching_sweatsuit
from zenas_athleisure_db.products.sweatband_coordination;
select product_code, headband_description, wristband_description
from zenas_athleisure_db.products.sweatband_product_line;
select sizes_available
from zenas_athleisure_db.products.sweatsuit_sizes;

//we good 😊 
