{{ config(materialized='table') }}
select cast(vendor_id as int) as vendor_id, vendor_name
from {{ ref('seed_vendor') }}