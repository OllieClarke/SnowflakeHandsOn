use role sysadmin;
use warehouse compute_wh;
use database mels_smoothie_challenge_db;
use schema trails;

//get length of trail
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
,st_length(to_geography(my_linestring)) as length_of_trail 
from cherry_creek_trail
group by trail_name;

//now for denvers trails
select
feature_name
, st_length(to_geography(whole_object)) as wo_length
, st_length(to_geography(geometry)) as geom_length
from denver_area_trails;

//update the view with these lengths
create or replace view denver_area_trails 
as (
select
$1:features[0]:properties:Name::string as feature_name
, $1:features[0]:geometry:coordinates::string as feature_coordinates
, $1:features[0]:geometry::string as geometry
, st_length(to_geography(geometry)) as trail_length
, $1:features[0]:properties::string as feature_properties
, $1:crs:properties:name::string as specs
, $1 as whole_object
from @trails_geojson 
(file_format => ff_json)
);

select * from denver_area_trails;

//make a view to make the wkt data more like geojson
create or replace view denver_area_trails_2
as(
select
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',')
within group (order by point_id)
||'],"type":"LineString"}' as geometry
, st_length(to_geography(geometry)) as trail_length
from cherry_creek_trail
group by trail_name
);

//bring it all together
select 
feature_name
, geometry
, trail_length
from denver_area_trails
union all
select
feature_name
, geometry
, trail_length
from 
denver_area_trails_2;

//set the types
select 
feature_name
, to_geography(geometry) as my_linestring
, trail_length
from denver_area_trails
union all
select
feature_name
, to_geography(geometry) as my_linestring
, trail_length
from 
denver_area_trails_2;

//let's use more spatial functions in a view
create or replace view trails_and_boundaries as
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from denver_area_trails
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from denver_area_trails_2;

select * from trails_and_boundaries;

//find the mins and maxes to make a bounding rectangle
select
min(min_eastwest) as western_edge
,min(min_northsouth) as southern_edge
,max(max_eastwest) as eastern_edge
,max(min_northsouth) as northern_edge
from trails_and_boundaries;

//make the br in wkt
select
'POLYGON(('||
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from trails_and_boundaries;
