use role sysadmin;
use database smoothies;
use schema public;

//Add a new colum to the orders table
alter table orders
add column name_on_order varchar(100);

//paste query from app to test
insert into smoothies.public.orders(ingredients,name_on_order) values ('Cantaloupe Figs Honeydew ','Ollieboi');

//see if the app works
select * from orders;

//add order filled column
alter table orders
add column order_filled boolean default false;

//update table to give some full values
update smoothies.public.orders
       set order_filled = true
       where name_on_order is null;
