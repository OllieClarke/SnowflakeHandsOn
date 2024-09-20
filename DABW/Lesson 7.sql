use role accountadmin;
use database util_db;
use schema public;

//set a variable
set mystery_bag = 'Nothing in here';

//call the variable
select $mystery_bag;

//set more vars
set var1=2;
set var2=5;
set var3=7;

select $var1+$var2+$var3;


//create a udf
create or replace function sum_mystery_bag_vars(var1 number, var2 number, var3 number)
returns number as 'select var1+var2+var3';

//test the udf
select sum_mystery_bag_vars (-5,12.14,2);

//using local vars
set eeny = 4;
set meeny = 67.2;
set miney_mo = -39;

select sum_mystery_bag_vars ($eeny, $meeny, $miney_mo);

//DORA Check
-- Set your worksheet drop lists

-- Set these local variables according to the instructions
set this = -10.5;
set that = 2;
set the_other =  1000;

-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW006' as step
 ,( select util_db.public.sum_mystery_bag_vars($this,$that,$the_other)) as actual
 , 991.5 as expected
 ,'Mystery Bag Function Output' as description
);


set alternating_caps_phrase = 'aLtErNaTiNg CaPs!';
select $alternating_caps_phrase;

//use the inbuilt initcap function
select initcap($alternating_caps_phrase);

//make a udf
create or replace function neutralize_whining(var1 text)
returns text as 'select initcap(var1)';

//test
select neutralize_whining ('ArE yOu SuRe ThAtS a GoOd IdEa?');

-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DABW007' as step
 ,( select hash(neutralize_whining('bUt mOm i wAsHeD tHe dIsHes yEsTeRdAy'))) as actual
 , -4759027801154767056 as expected
 ,'WHINGE UDF Works' as description
);
