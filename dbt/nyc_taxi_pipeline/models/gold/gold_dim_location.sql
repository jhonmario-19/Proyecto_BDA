-- gold_dim_location.sql
-- Dimension geografica: 263 zonas de taxi predefinidas por el TLC de NYC
-- Fuente: /databricks-datasets/nyctaxi/taxizone/taxi+_zone_lookup.csv
-- Esta dimension se usa DOBLE en la fact table (patron role-playing):
--   gold_fact_trips.pu_location_id -> gold_dim_location (zona de recogida)
--   gold_fact_trips.do_location_id -> gold_dim_location (zona de destino)
-- En Power BI se crean dos relaciones: una activa (pickup) y una inactiva
-- (dropoff), alternando con USERELATIONSHIP() en las medidas DAX.

{{
    config(
        materialized='table',
        schema='gold',
        file_format='delta',
        tags=['gold', 'dimension', 'geographic']
    )
}}


WITH zonas_raw AS (
    SELECT
        LocationID,
        Borough,
        Zone,
        service_zone
    FROM read_files('/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv',
		format => 'csv', header => true)
),

zonas_limpias AS (
    SELECT
        CAST(LocationID AS INT)                 AS location_id,
        TRIM(Zone)                              AS zone_name,
        TRIM(Borough)                           AS borough,
        TRIM(service_zone)                      AS service_zone,
        -- Agrupacion regional simplificada para dashboards ejecutivos
        CASE
            WHEN TRIM(Borough) = 'Manhattan'    THEN 'Manhattan'
            WHEN TRIM(Borough) = 'Brooklyn'     THEN 'Outer Boroughs'
            WHEN TRIM(Borough) = 'Queens'       THEN 'Outer Boroughs'
            WHEN TRIM(Borough) = 'Bronx'        THEN 'Outer Boroughs'
            WHEN TRIM(Borough) = 'Staten Island' THEN 'Outer Boroughs'
            WHEN TRIM(Borough) = 'EWR'          THEN 'Airports'
            ELSE 'Desconocido'
        END                                     AS region_group
    FROM zonas_raw
    WHERE LocationID IS NOT NULL
)

SELECT
    location_id,
    zone_name,
    borough,
    service_zone,
    region_group
FROM zonas_limpias
ORDER BY location_id
