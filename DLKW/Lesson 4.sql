use database zenas_athleisure_db;
use schema products;

//what's in the other stage
list @sweatsuits;

select $1 
from @sweatsuits/purple_sweatsuit.png;
/*this is opening the file as though it's a text file and trying to read it in.
this obviously doesn't work*/

select metadata$filename
, COUNT(metadata$file_row_number) as number_of_rows
from @sweatsuits/purple_sweatsuit.png
GROUP BY metadata$filename;


//directory table
select *
from directory(@sweatsuits);

//do functions work on directory tables?
select REPLACE(relative_path, '_', ' ') as no_underscores_filename
, REPLACE(no_underscores_filename, '.png') as just_words_filename
, INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);

//cool that snowflake can immediately reference aliases
select initcap(replace(replace(relative_path,'_',' '),'.png')) as product_name
from directory(@sweatsuits);

//make a table for suit info
create or replace table sweatsuits (
	color_or_style varchar(25),
	file_name varchar(50),
	price number(5,2)
);

//put data into the table
insert into  sweatsuits 
          (color_or_style, file_name, price)
values
 ('Burgundy', 'burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'forest_green_sweatsuit.png',64)
,('Navy Blue', 'navy_blue_sweatsuit.png',65)
,('Orange', 'orange_sweatsuit.png',65)
,('Pink', 'pink_sweatsuit.png',63)
,('Purple', 'purple_sweatsuit.png',64)
,('Red', 'red_sweatsuit.png',68)
,('Royal Blue',	'royal_blue_sweatsuit.png',65)
,('Yellow', 'yellow_sweatsuit.png',67);

//you can join tables onto directories
select * from directory(@sweatsuits) a
join sweatsuits b
on a.relative_path = b.file_name;

//make a nice view
create or replace view product_list as
select initcap(replace(replace(a.relative_path,'_',' '),'.png')) as product_name,
b.file_name,
color_or_style,
price,
file_url
from 
directory(@sweatsuits) a
join sweatsuits b
on a.relative_path = b.file_name;

select * from product_list;

//get every size for every product
select * 
from product_list p
cross join sweatsuit_sizes;

//make a catalog
create or replace view catalog as
select *
from product_list
cross join sweatsuit_sizes;

select * from catalog;

-- Add a table to map the sweatsuits to the sweat band sets
create or replace table upsell_mapping
(
sweatsuit_color_or_style varchar(25)
,upsell_product_code varchar(10)
);

--populate the upsell table
insert into upsell_mapping
(
sweatsuit_color_or_style
,upsell_product_code 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');

//zena's code
-- Zena needs a single view she can query for her website prototype
create view catalog_for_website as 
select color_or_style
,price
,file_name
, get_presigned_url(@sweatsuits, file_name, 3600) as file_url
,size_list
,coalesce('Consider: ' ||  headband_description || ' & ' || wristband_description, 'Consider: White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, file_name
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, file_name
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code;

select * from catalog_for_website;
