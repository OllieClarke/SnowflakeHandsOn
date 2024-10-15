use database mels_smoothie_challenge_db;
use schema trails;

//Parse out parquet
select 
$1:sequence_1 as sequence_1
,$1:sequence_2 as sequence_2
,$1:trail_name::varchar as trail_name
,$1:latitude as latitude
,$1:longitude as longitude
,$1:elevation as elevation
from @trails_parquet
(file_format => ff_parquet)
order by sequence_1;

//format data
select 
$1:sequence_1 as point_id
,$1:trail_name::varchar as trail_name
,$1:latitude::number(11,8) as lng
,$1:longitude::number(11,8)  as lat
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

//make a nice view
create or replace view cherry_creek_trail
as (select 
$1:sequence_1::number as point_id
,$1:trail_name::varchar as trail_name
,$1:latitude::number(11,8) as lng
,$1:longitude::number(11,8)  as lat
from @trails_parquet
(file_format => ff_parquet)
order by point_id);

//prepare for wkt
select top 100
lng||' '||lat as coord_pair
, 'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;

//add to the view
create or replace view cherry_creek_trail
as (select 
$1:sequence_1::number as point_id
,$1:trail_name::varchar as trail_name
,$1:latitude::number(11,8) as lng
,$1:longitude::number(11,8)  as lat
,lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id);

//constructing linestring for wkt
select
'LINESTRING('||listagg(coord_pair,',')
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
--where point_id<=2450
group by trail_name;


//now for json
select $1
from @trails_geojson
(file_format => ff_json);

//parse the json a bit
select
$1:features[0]:properties:Name::string as feature_name
, $1:features[0]:geometry:coordinates::string as feature_coordinates
, $1:features[0]:geometry::string as geometry
, $1:features[0]:properties::string as feature_properties
, $1:crs:properties:name::string as specs
, $1 as whole_object
from @trails_geojson 
(file_format => ff_json);

//make a view
create or replace view denver_area_trails 
as (
select
$1:features[0]:properties:Name::string as feature_name
, $1:features[0]:geometry:coordinates::string as feature_coordinates
, $1:features[0]:geometry::string as geometry
, $1:features[0]:properties::string as feature_properties
, $1:crs:properties:name::string as specs
, $1 as whole_object
from @trails_geojson 
(file_format => ff_json)
);
