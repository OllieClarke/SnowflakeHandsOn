//create new table
use database smoothies;
use schema public;

create or replace table orders
(ingredients varchar(200));

//test insert statement
insert into smoothies.public.orders(ingredients) values ('Blueberries Elderberries Jackfruit ');

//check it worked
select * from orders;

//clear the table
truncate table orders;
