-- gold_dim_payment.sql
-- Dimension de tipos de pago (tabla estatica: 6 filas)
-- La columna generates_tip es CRITICA para el KPI 2 (Tasa de Propina):
-- filtra solo los pagos con tarjeta donde la propina queda registrada
-- electronicamente, evitando sesgar el promedio con efectivo (propina
-- en efectivo no se registra en el sistema, aparece como 0.00).

{{
    config(
        materialized='table',
        schema='gold',
        file_format='delta',
        tags=['gold', 'dimension', 'static']
    )
}}

SELECT
    payment_id,
    payment_code,
    payment_description,
    generates_tip,
    is_digital
FROM (
    VALUES
        (-1, 'UNK',  'Desconocido',          FALSE, FALSE),
        ( 1, 'CRD',  'Tarjeta de credito',   TRUE,  TRUE),
        ( 2, 'CSH',  'Efectivo',             FALSE, FALSE),
        ( 3, 'NOC',  'Sin cargo',            FALSE, TRUE),
        ( 4, 'DIS',  'Disputa',              FALSE, TRUE),
        ( 5, 'OTH',  'Otro',                 FALSE, FALSE)
) AS t(payment_id, payment_code, payment_description, generates_tip, is_digital)
