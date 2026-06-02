-- gold_dim_datetime.sql
-- Dimension temporal generada desde silver_trips
-- Granularidad: un registro por MINUTO UNICO de pickup
-- Justificacion: truncar al minuto reduce la dimension de ~359M filas
-- a un maximo de ~525,600 filas (minutos en un año × 3.5 años)
-- lo que es manejable en memoria para Power BI via Direct Query.

{{
    config(
        materialized='table',
        schema='gold',
        file_format='delta',
        partition_by='year',
        tags=['gold', 'dimension', 'temporal']
    )
}}

WITH fechas_unicas AS (
    SELECT DISTINCT
        DATE_TRUNC('minute', pickup_datetime) AS pickup_minute
    FROM {{ ref('silver_trips') }}
    WHERE pickup_datetime IS NOT NULL
),

dimensiones AS (
    SELECT
        -- Surrogate key: entero compacto basado en el minuto
        -- Formato: YYYYMMDDHHMM → entero de 12 digitos
        CAST(
            DATE_FORMAT(pickup_minute, 'yyyyMMddHHmm')
        AS BIGINT) AS datetime_id,

        pickup_minute                                       AS pickup_datetime,
        YEAR(pickup_minute)                                 AS year,
        QUARTER(pickup_minute)                              AS quarter,
        MONTH(pickup_minute)                                AS month,
        DATE_FORMAT(pickup_minute, 'MMMM')                  AS month_name,
        DAY(pickup_minute)                                  AS day,
        HOUR(pickup_minute)                                 AS hour,
        MINUTE(pickup_minute)                               AS minute,
        DAYOFWEEK(pickup_minute)                            AS day_of_week,
        DATE_FORMAT(pickup_minute, 'EEEE')                  AS day_name,
        CASE
            WHEN DAYOFWEEK(pickup_minute) IN (1, 7) THEN TRUE
            ELSE FALSE
        END                                                 AS is_weekend,
        CASE
            WHEN HOUR(pickup_minute) BETWEEN 7 AND 9
              OR HOUR(pickup_minute) BETWEEN 17 AND 19
            THEN TRUE
            ELSE FALSE
        END                                                 AS is_rush_hour,
        CASE
            WHEN HOUR(pickup_minute) BETWEEN 0 AND 5  THEN 'madrugada'
            WHEN HOUR(pickup_minute) BETWEEN 6 AND 11 THEN 'mañana'
            WHEN HOUR(pickup_minute) BETWEEN 12 AND 17 THEN 'tarde'
            ELSE 'noche'
        END                                                 AS time_of_day,
        TO_DATE(pickup_minute)                              AS date_only
    FROM fechas_unicas
)

SELECT * FROM dimensiones
ORDER BY datetime_id
