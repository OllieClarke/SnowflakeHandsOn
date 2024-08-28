//rename ths sample database back
alter database that_really_cool_sample_stuff
rename to snowflake_sample_data;

//can I drop the account usage share?
drop database snowflake;
//no I can't

//can I rename it?
alter database snowflake
rename to something_else;
//no I can't


//Give privileges to sysadmin
grant imported privileges
on database snowflake_sample_data
to role sysadmin;

//Looking at sample data
--Check the range of values in the Market Segment Column
SELECT DISTINCT c_mktsegment
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

--Find out which Market Segments have the most customers
SELECT c_mktsegment, COUNT(*)
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
GROUP BY c_mktsegment
ORDER BY COUNT(*);

-- Nations Table
SELECT N_NATIONKEY, N_NAME, N_REGIONKEY
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

-- Regions Table
SELECT R_REGIONKEY, R_NAME
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

-- Join the Tables and Sort
SELECT R_NAME as Region, N_NAME as Nation
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
ORDER BY R_NAME, N_NAME ASC;

--Group and Count Rows Per Region
SELECT R_NAME as Region, count(N_NAME) as NUM_COUNTRIES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
GROUP BY R_NAME;
