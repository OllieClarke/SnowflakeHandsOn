use database smoothies;
use schema public;

//add a new column to fruit options table
alter table fruit_options
add column search_on varchar(100);

//copy the name to match, before updating
update fruit_options
set search_on = fruit_name;

//get everything
select * from fruit_options;

//jump into alteryx to compare to Api output and then update
update fruit_options
set search_on = 'Apple'
WHERE fruit_name = 'Apples';

update fruit_options
set search_on = 'Fig'
WHERE fruit_name = 'Figs';

update fruit_options
set search_on = 'Raspberry'
WHERE fruit_name = 'Raspberries';

update fruit_options
set search_on = 'Strawberry'
WHERE fruit_name = 'Strawberries';

update fruit_options
set search_on = 'Blueberry'
WHERE fruit_name = 'Blueberries';

update fruit_options
set search_on = 'Dragonfruit'
WHERE fruit_name = 'Dragon Fruit';

//get everything
select * from fruit_options;
