--create db
use role sysadmin;
create or replace database util_db;

--ensure ownership is correct
use role accountadmin;
grant ownership on database util_db to role sysadmin;
grant ownership on warehouse compute_wh to role sysadmin;
grant ownership on schema util_db.public to role sysadmin;
use role sysadmin;

--ensure warehouse is setup correctly
alter warehouse compute_wh set warehouse_size = xsmall;
alter warehouse compute_wh set auto_suspend = 150;

--ensure my defaults are correct
alter user ollieclarke set default_role = 'SYSADMIN';
alter user ollieclarke set default_warehouse = 'COMPUTE_WH';
alter user ollieclarke set default_namespace = 'UTIL_DB.PUBLIC';

--run dora script
use role accountadmin;
create or replace api integration dora_api_integration api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole' enabled = true api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');
create or replace external function util_db.public.grader(        
 step varchar     
 , passed boolean     
 , actual integer     
 , expected integer    
 , description varchar) 
 returns variant 
 api_integration = dora_api_integration 
 context_headers = (current_timestamp, current_account, current_statement, current_account_name) 
 as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'  
;  

--confirm dora is working
use role accountadmin;
select util_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 