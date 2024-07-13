--
with source as (

    select * from {{ source('main', 'fhv_bases') }}

),

renamed as (

    select
        -- trim base_num to properly match foreign key
        trim(upper(base_number)) as base_number,
        base_name,
        dba,
        dba_category,
        filename

    from source

)

select * from renamed
