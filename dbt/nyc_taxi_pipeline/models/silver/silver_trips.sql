{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

-- ============================================================
-- SILVER: silver_trips
-- Limpieza y normalización de Bronze v2 (2016-2019)
-- 431 millones de registros con esquema Location IDs
-- ============================================================

WITH bronze_v2 AS (
    SELECT
        VendorID                                        AS vendor_id,
        CAST(tpep_pickup_datetime AS TIMESTAMP)         AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP)        AS dropoff_datetime,
        TRY_CAST(passenger_count AS INT)                AS passenger_count,
        TRY_CAST(trip_distance AS DOUBLE)               AS trip_distance,
        TRY_CAST(RatecodeID AS INT)                     AS ratecode_id,
        store_and_fwd_flag                              AS store_and_fwd_flag,
        TRY_CAST(PULocationID AS INT)                   AS pu_location_id,
        TRY_CAST(DOLocationID AS INT)                   AS do_location_id,
        TRY_CAST(payment_type AS INT)                   AS payment_type,
        TRY_CAST(fare_amount AS DOUBLE)                 AS fare_amount,
        TRY_CAST(extra AS DOUBLE)                       AS extra,
        TRY_CAST(mta_tax AS DOUBLE)                     AS mta_tax,
        TRY_CAST(tip_amount AS DOUBLE)                  AS tip_amount,
        TRY_CAST(tolls_amount AS DOUBLE)                AS tolls_amount,
        TRY_CAST(improvement_surcharge AS DOUBLE)       AS improvement_surcharge,
        TRY_CAST(congestion_surcharge AS DOUBLE)        AS congestion_surcharge,
        TRY_CAST(total_amount AS DOUBLE)                AS total_amount

    FROM {{ source('bronze', 'nyc_taxi_v2') }}
),

limpio AS (
    SELECT
        -- ID único por viaje

		MD5(CONCAT(
			COALESCE(vendor_id, ''),
			COALESCE(CAST(pickup_datetime AS STRING), ''),
			COALESCE(CAST(dropoff_datetime AS STRING), ''),
			COALESCE(CAST(trip_distance AS STRING), ''),
			COALESCE(CAST(payment_type AS STRING), ''),
			COALESCE(CAST(pu_location_id AS STRING), ''),
			COALESCE(CAST(do_location_id AS STRING), ''),
			COALESCE(CAST(total_amount AS STRING), '')
		))                                          AS trip_id,

		-- vendor_id: permitir nulos en lugar de fallar
		TRY_CAST(vendor_id AS INT)                 AS vendor_id,

        -- Timestamps validados
        pickup_datetime,
        dropoff_datetime,

        -- Duración en minutos calculada
        ROUND(
            (UNIX_TIMESTAMP(dropoff_datetime) - UNIX_TIMESTAMP(pickup_datetime)) / 60, 2
        )                                           AS trip_duration_min,

        -- Pasajeros: nulos → 1, fuera de rango → null
        CASE
            WHEN passenger_count IS NULL THEN 1
            WHEN passenger_count BETWEEN 1 AND 6 THEN passenger_count
            ELSE NULL
        END                                         AS passenger_count,

        -- Distancia: ceros y negativos → null
        CASE
            WHEN trip_distance > 0 THEN trip_distance
            ELSE NULL
        END                                         AS trip_distance,

        ratecode_id,
        store_and_fwd_flag,
        payment_type,
        pu_location_id,
        do_location_id,

        -- Tarifas: negativos → null
        CASE WHEN fare_amount >= 0 THEN fare_amount ELSE NULL END       AS fare_amount,
        CASE WHEN extra >= 0 THEN extra ELSE NULL END                   AS extra,
        CASE WHEN mta_tax >= 0 THEN mta_tax ELSE NULL END               AS mta_tax,
        CASE WHEN tip_amount >= 0 THEN tip_amount ELSE NULL END         AS tip_amount,
        CASE WHEN tolls_amount >= 0 THEN tolls_amount ELSE NULL END     AS tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        CASE WHEN total_amount >= 0 THEN total_amount ELSE NULL END     AS total_amount

    FROM bronze_v2
    WHERE
        pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND dropoff_datetime > pickup_datetime
        AND YEAR(pickup_datetime) BETWEEN 2016 AND 2019
)

SELECT * FROM limpio