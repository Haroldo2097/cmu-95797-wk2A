-- 4 Attributes need cleanup : Seee below
with source as (

    select * from {{ source('main', 'green_tripdata') }}

),

renamed as (

    select
        vendorid,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        -- Ensure this is a 0/1
        {{flag_to_bool("store_and_fwd_flag")}} as store_and_fwd_flag,        
        ratecodeid,
        pulocationid,
        dolocationid,
        passenger_count::int as passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        --Trim null data
        --ehail_fee, 
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,
        filename

    from source
      WHERE lpep_pickup_datetime < TIMESTAMP '2022-12-31' -- drop future rows
        AND trip_distance >= 0 -- only record pos trip distance data
)

select * from renamed