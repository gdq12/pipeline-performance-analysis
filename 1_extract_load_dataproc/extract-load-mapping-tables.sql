-- create needed schema 
create schema if not exists `${PROJECT_ID}`.`nytaxi_mapping`
options (location = 'EU')
;

-- bring data from bucket to then push to mapping schema 
create or replace external table `${PROJECT_ID}`.`nytaxi_raw_backup`.`mapping_data`
options (
format = 'CSV',
uris = ['gs://helper-data2/bigquery_public_data_taxi_zone_geom.csv']
)
;

create table if not exists `${PROJECT_ID}`.`nytaxi_mapping`.`taxi_zone_geom`
(zone_id integer, zone_name string, borough string, zone_geom geography, creation_dt timestamp) as
select 
  zone_id, zone_name
  , borough
  , ST_GEOGFROMTEXT(zone_geom) zone_geom
  , current_timestamp()
from `${PROJECT_ID}`.`nytaxi_raw_backup`.`mapping_data`
;

create or replace external table `${PROJECT_ID}`.`nytaxi_raw_backup`.`mapping_data`
options (
format = 'CSV',
uris = ['gs://helper-data2/hvlv_base_numbers.csv'],
skip_leading_rows = 1
)
;

create table if not exists `${PROJECT_ID}`.`nytaxi_mapping`.`hvlv_base_numbers`
(hvln string, license_base_number string, base_name string, app_company_affiliation string, creation_dt timestamp) as
select *, current_timestamp()
from `${PROJECT_ID}`.`nytaxi_raw_backup`.`mapping_data`
;

create or replace external table `${PROJECT_ID}`.`nytaxi_raw_backup`.`mapping_data`
options (
format = 'CSV',
uris = ['gs://helper-data2/taxi_zone_lookup.csv']
)
;

create table if not exists `${PROJECT_ID}`.`nytaxi_mapping`.`taxi_zone_lookup`
(location_id integer, borough string, zone string, service_zone string, creation_dt timestamp) as
select *, current_timestamp()
from `${PROJECT_ID}`.`nytaxi_raw_backup`.`mapping_data`
;