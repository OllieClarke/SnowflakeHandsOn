use role sysadmin;
use database smoothies;
use schema public;

//Create a sequence
create or replace sequence order_seq
    start = 1
    increment = 2
    ORDER
    comment = 'Provide a unique id for each smoothie order';


//truncate orders
truncate orders;

//add the unique id
alter table orders
add column order_uid integer --create the column
default order_seq.nextval --add the sequence as the value
constraint order_uid unique enforced; --make sure it's unique


//ensure the orders table is okay
drop table orders;
create or replace table smoothies.public.orders (
       order_uid integer default smoothies.public.order_seq.nextval,
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       constraint order_uid unique (order_uid),
       order_ts timestamp_ltz default current_timestamp()
);

//check
select * from orders;
