-- gold_fact_trips.sql
-- Tabla de hechos central del Star Schema NYC Yellow Taxi
-- Granularidad: UN VIAJE COMPLETADO = UNA FILA
-- 359,597,254 registros esperados (provenientes de silver_trips)
--
-- DECISIONES DE DISEÑO:
-- 1. datetime_id se genera con DATE_FORMAT al minuto (YYYYMMDDHHMM)
--    para hacer JOIN eficiente con dim_datetime sin buscar por TIMESTAMP
-- 2. Se incluyen SOLO metricas aditivas en la fact table
--    (fare_amount, mta_tax, extra se excluyen: son contables fiscales,
--     no KPIs de negocio segun la metodologia Kimball)
-- 3. congestion_surcharge se agrega como metrica opcional (disponible
--    desde 2019; será NULL para registros 2016-2018, lo cual es correcto)
-- 4. fact_id usa ROW_NUMBER para garantizar unicidad absoluta

{{
    config(
        materialized='table',
        schema='gold',
        file_format='delta',
        partition_by='year',
        cluster_by=['pu_location_id', 'vendor_id'],
        tags=['gold', 'fact', 'core']
    )
}}

WITH silver AS (
    SELECT
        trip_id,
        pickup_datetime,
        dropoff_datetime,
        vendor_id,
        payment_type          AS payment_id,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        trip_duration_min,
        tip_amount,
        total_amount,
        congestion_surcharge,
        YEAR(pickup_datetime) AS year
    FROM {{ ref('silver_trips') }}
    WHERE
        pickup_datetime IS NOT NULL
        AND total_amount IS NOT NULL
        AND trip_distance IS NOT NULL
        AND trip_distance > 0
        AND total_amount >= 0
        AND trip_duration_min > 0
)

SELECT
    -- Surrogate key de la fact table
    ROW_NUMBER() OVER (ORDER BY pickup_datetime, trip_id) AS fact_id,

    -- Claves foraneas hacia las dimensiones
    CAST(
        DATE_FORMAT(pickup_datetime, 'yyyyMMddHHmm')
    AS BIGINT)                                               AS datetime_id,

    COALESCE(vendor_id, -1)                               AS vendor_id,
    COALESCE(payment_id, -1)                              AS payment_id,
    COALESCE(pu_location_id, 264)                         AS pu_location_id,
    COALESCE(do_location_id, 264)                         AS do_location_id,

    -- Metricas aditivas del negocio
    COALESCE(CAST(passenger_count AS INT), 1)             AS passenger_count,
    ROUND(CAST(trip_distance AS DOUBLE), 2)               AS trip_distance,
    ROUND(CAST(trip_duration_min AS DOUBLE), 2)           AS trip_duration_min,
    COALESCE(ROUND(CAST(tip_amount AS DOUBLE), 2), 0.0)   AS tip_amount,
    ROUND(CAST(total_amount AS DOUBLE), 2)                AS total_amount,

    -- Metrica opcional: disponible desde 2019
    ROUND(COALESCE(CAST(congestion_surcharge AS DOUBLE), 0.0), 2) AS congestion_surcharge,

    -- Campo de particion (replica de YEAR para que Spark lo use)
    year

FROM silver
