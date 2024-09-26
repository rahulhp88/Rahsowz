{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select
        id,
        attribute_val,
        updated_at
    from {{ ref('xyz_source') }}  -- Replace with your source table reference
),

-- Get the current records from the existing SCD2 table
current_records as (
    select *
    from {{ this }}
    where current_flag = 'Y'
),

-- Detect changes by comparing the source and current records
changes as (
    select
        source.id,
        source.attribute_val,
        source.updated_at,
        current.id as current_id,
        current.attribute_val as current_attribute_val,
        current.updated_at as current_updated_at
    from source
    left join current_records current
    on source.id = current.id
    where current.id is null -- New records
    or source.attribute_val != current.attribute_val -- Changed records
)

-- Insert changed or new records into the SCD2 table
select
    changes.id,
    changes.attribute_val,
    changes.updated_at as start_date,
    null as end_date,
    'Y' as current_flag,
    changes.updated_at
from changes

{% if is_incremental() %}
-- For incremental models, expire old records if there are changes
union all
select
    current.id,
    current.attribute_val,
    current.start_date,
    current.updated_at as end_date,
    'N' as current_flag,
    current.updated_at
from current_records current
join changes on current.id = changes.id
{% endif %}
