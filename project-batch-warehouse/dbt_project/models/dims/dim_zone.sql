{{ config(materialized='table') }}

select 
    cast(zone_id as int) as zone_id,
    borough,
    zone_name
from {{ ref('seed_zone') }}