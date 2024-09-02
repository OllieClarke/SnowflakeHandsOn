//What countries are included in the history_day view?
select distinct country
from history_day;

//What postal codes are available in detroit? (detroit being 481... or 482...)
select distinct postal_code
from history_day
where
country = 'US' AND (
postal_code LIKE '481%'
OR postal_code LIKE '482%');

//create a new database
create or replace database marketing;

//create a schema within marketing
use marketing;
create or replace schema mailers;

//Create a view
create or replace view DETROIT_ZIPS as
(
select distinct postal_code
from weathersource.standard_tile.history_day a
WHERE country = 'US'
AND (postal_code like '481%'
OR postal_code like '482%')
);

//how many rows in history day?
select count(*)
from weathersource.standard_tile.history_day;

//how many in detroit?
select count(*)
from weathersource.standard_tile.history_day a
JOIN marketing.mailers.detroit_zips b
on a.postal_code = b.postal_code;


//Use the detroit view to filter history day to detroit
select a.*
from weathersource.standard_tile.history_day a
JOIN marketing.mailers.detroit_zips b
on a.postal_code = b.postal_code;

//what's the date range on the dataset?
use database weathersource;
use schema standard_tile;
select min(date_valid_std) as "start"
, max(date_valid_std) as "end"
from history_day a
JOIN marketing.mailers.detroit_zips b
on a.postal_code = b.postal_code;

//look at forecast date range
select min(date_valid_std) as "start"
, max(date_valid_std) as "end"
from forecast_day a
JOIN marketing.mailers.detroit_zips b
on a.postal_code = b.postal_code;


//Which day in the next two weeks is best for a sale?
select a.date_valid_std, avg(a.avg_cloud_cover_tot_pct) as "avg_cloud_cover_tot_pct"
from forecast_day a
JOIN marketing.mailers.detroit_zips b
on a.postal_code = b.postal_code
GROUP BY a.date_valid_std
--order by lowest cloud cover, then soonest day
ORDER BY avg(a.avg_cloud_cover_tot_pct) asc, date_valid_std asc
--LIMIT 1;
;
