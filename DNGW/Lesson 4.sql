use database ags_game_audience;
use schema raw;

//using Kishore's IP
select parse_ip('100.41.16.160','inet');

//pull out ipv4
select parse_ip('100.41.16.160','inet'):ipv4;

//make the enhanced schema
create or replace schema enhanced;


//look up kishore's Ip in the ipinfo db
select
start_ip
,end_ip
,start_ip_int
,end_ip_int
,city
,region
,country
,timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160','inet'):ipv4 between start_ip_int and end_ip_int;


//get everyone's timezone
select
a.*
,b.city
,b.region
,b.country
,b.timezone
from 
ags_game_audience.raw.logs a
join 
IPINFO_GEOLOC.demo.location b
on parse_ip(a.ip_address,'inet'):ipv4 between b.start_ip_int and b.end_ip_int;

//let's be more efficient
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

//and add local timestamp, day of week
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone
, convert_timezone('UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
--where user_login like'%prajina%'
;

//make a lookup table of times of day
create or replace table time_of_day_lu
(hour number
, tod_name varchar(25));

//fill it
insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

//look at the table
select tod_name, listagg(hour,',') 
from time_of_day_lu
group by tod_name;

//add the time of day to the sql from earlier
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, city
, region
, country
, timezone as gamer_ltz_name
, convert_timezone('UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
, tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN time_of_day_lu tod
ON hour(game_event_ltz) = tod.hour;

//make a table of this
create or replace table ags_game_audience.enhanced.logs_enhanced
as(
SELECT logs.ip_address
, logs.user_login as gamer_name
, logs.user_event as game_event_name
, logs.datetime_iso8601 as game_event_utc
, city
, region
, country
, timezone as gamer_ltz_name
, convert_timezone('UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, dayname(game_event_ltz) as dow_name
, tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN time_of_day_lu tod
ON hour(game_event_ltz) = tod.hour
);

select * from ags_game_audience.enhanced.logs_enhanced;
