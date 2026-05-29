{{ config(
    materialized='table',
    file_format='delta',
    schema='silver'
) }}

-- ============================================================
-- SILVER: silver_trips
-- Unifica Bronze v1 (2009-2015) y Bronze v2 (2016-2019)
-- en un esquema común limpio y normalizado
-- ============================================================

WITH bronze_v1 AS (
    SELECT
        -- Identificación
        vendor_id                                   AS vendor_id,
        'v1_gps'                                    AS schema_version,

        -- Timestamps
        CAST(pickup_datetime AS TIMESTAMP)          AS pickup_datetime,
        CAST(dropoff_datetime AS TIMESTAMP)         AS dropoff_datetime,

        -- Métricas del viaje
        CAST(passenger_count AS INT)                AS passenger_count,
        CAST(trip_distance AS DOUBLE)               AS trip_distance,
        CAST(rate_code AS INT)                      AS ratecode_id,
        store_and_fwd_flag                          AS store_and_fwd_flag,

        -- Ubicación GPS (solo v1)
        CAST(pickup_longitude AS DOUBLE)            AS pickup_longitude,
        CAST(pickup_latitude AS DOUBLE)             AS pickup_latitude,
        CAST(dropoff_longitude AS DOUBLE)           AS dropoff_longitude,
        CAST(dropoff_latitude AS DOUBLE)            AS dropoff_latitude,

        -- Location IDs (null en v1)
        CAST(NULL AS INT)                           AS pu_location_id,
        CAST(NULL AS INT)                           AS do_location_id,

        -- Pago — estandarizar texto a número
        CASE UPPER(TRIM(payment_type))
            WHEN 'CREDIT' THEN 1
            WHEN 'CREDIT CARD' THEN 1
            WHEN 'CASH' THEN 2
            WHEN 'NO CHARGE' THEN 3
            WHEN 'DISPUTE' THEN 4
            ELSE 5
        END                                         AS payment_type,

        -- Tarifas
        CAST(fare_amount AS DOUBLE)                 AS fare_amount,
        CAST(surcharge AS DOUBLE)                   AS extra,
        CAST(mta_tax AS DOUBLE)                     AS mta_tax,
        CAST(tip_amount AS DOUBLE)                  AS tip_amount,
        CAST(tolls_amount AS DOUBLE)                AS tolls_amount,
        CAST(NULL AS DOUBLE)                        AS improvement_surcharge,
        CAST(NULL AS DOUBLE)                        AS congestion_surcharge,
        CAST(total_amount AS DOUBLE)                AS total_amount

    FROM {{ source('bronze', 'nyc_taxi_v1') }}
),

bronze_v2 AS (
    SELECT
        -- Identificación
        VendorID                                    AS vendor_id,
        'v2_locationid'                             AS schema_version,

        -- Timestamps
        CAST(tpep_pickup_datetime AS TIMESTAMP)     AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP)    AS dropoff_datetime,

        -- Métricas del viaje
        CAST(passenger_count AS INT)                AS passenger_count,
        CAST(trip_distance AS DOUBLE)               AS trip_distance,
        CAST(RatecodeID AS INT)                     AS ratecode_id,
        store_and_fwd_flag                          AS store_and_fwd_flag,

        -- Ubicación GPS (null en v2)
        CAST(NULL AS DOUBLE)                        AS pickup_longitude,
        CAST(NULL AS DOUBLE)                        AS pickup_latitude,
        CAST(NULL AS DOUBLE)                        AS dropoff_longitude,
        CAST(NULL AS DOUBLE)                        AS dropoff_latitude,

        -- Location IDs (solo v2)
        CAST(PULocationID AS INT)                   AS pu_location_id,
        CAST(DOLocationID AS INT)                   AS do_location_id,

        -- Pago — ya viene como número
        CAST(payment_type AS INT)                   AS payment_type,

        -- Tarifas
        CAST(fare_amount AS DOUBLE)                 AS fare_amount,
        CAST(extra AS DOUBLE)                       AS extra,
        CAST(mta_tax AS DOUBLE)                     AS mta_tax,
        CAST(tip_amount AS DOUBLE)                  AS tip_amount,
        CAST(tolls_amount AS DOUBLE)                AS tolls_amount,
        CAST(improvement_surcharge AS DOUBLE)       AS improvement_surcharge,
        CAST(congestion_surcharge AS DOUBLE)        AS congestion_surcharge,
        CAST(total_amount AS DOUBLE)                AS total_amount

    FROM {{ source('bronze', 'nyc_taxi_v2') }}
),

unificado AS (
    SELECT * FROM bronze_v1
    UNION ALL
    SELECT * FROM bronze_v2
),

limpio AS (
    SELECT
        -- Generar ID único por viaje
        MD5(CONCAT(
            COALESCE(vendor_id, ''),
            COALESCE(CAST(pickup_datetime AS STRING), ''),
            COALESCE(CAST(dropoff_datetime AS STRING), ''),
            COALESCE(CAST(trip_distance AS STRING), '')
        ))                                          AS trip_id,

        -- Vendor normalizado
        CAST(vendor_id AS INT)                      AS vendor_id,
        schema_version,

        -- Timestamps validados
        pickup_datetime,
        dropoff_datetime,

        -- Duración calculada en minutos
        ROUND(
            (UNIX_TIMESTAMP(dropoff_datetime) - UNIX_TIMESTAMP(pickup_datetime)) / 60, 2
        )                                           AS trip_duration_min,

        -- Pasajeros: nulos → 1, fuera de rango → null
        CASE
            WHEN passenger_count IS NULL THEN 1
            WHEN passenger_count BETWEEN 1 AND 6 THEN passenger_count
            ELSE NULL
        END                                         AS passenger_count,

        -- Distancia: negativos y ceros → null
        CASE
            WHEN trip_distance > 0 THEN trip_distance
            ELSE NULL
        END                                         AS trip_distance,

        ratecode_id,
        store_and_fwd_flag,
        payment_type,
        pu_location_id,
        do_location_id,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,

        -- Tarifas: negativos → null
        CASE WHEN fare_amount >= 0 THEN fare_amount ELSE NULL END       AS fare_amount,
        CASE WHEN extra >= 0 THEN extra ELSE NULL END                   AS extra,
        CASE WHEN mta_tax >= 0 THEN mta_tax ELSE NULL END               AS mta_tax,
        CASE WHEN tip_amount >= 0 THEN tip_amount ELSE NULL END         AS tip_amount,
        CASE WHEN tolls_amount >= 0 THEN tolls_amount ELSE NULL END     AS tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        CASE WHEN total_amount >= 0 THEN total_amount ELSE NULL END     AS total_amount

    FROM unificado
    WHERE
        -- Filtros de calidad básicos
        pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND dropoff_datetime > pickup_datetime
        AND YEAR(pickup_datetime) BETWEEN 2009 AND 2019
)

SELECT * FROM limpio